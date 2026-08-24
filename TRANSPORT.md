# 多进程与分布式传输 —— 实现文档

> 本文是 `DESIGN.md` 的传输层实现配套：回答"如何把 actor 从 Web Worker 扩展到
> 多进程（`Deno.Command` / `node:child_process`）与分布式（WebSocket / TCP /
> 任意二进制流通道）"，并给出分阶段实施计划。所有"已验证事实"均为 2026-08 在
> Deno 2.9.5 / Node 26 上的实测结论。

## 0. TL;DR

- **统一模型**：Worker、子进程、WebSocket、TCP 都是同一个 **Transport（连接）**
  的实现。一个 Transport 既承载主消息通道，也用 `openChannel()` 新建逻辑通道
  （多路复用）；**MessagePort 只是 messageport 型 Transport 的一种实现** （它的
  openChannel 恰好产出可 transfer 的端口），不是特殊 API。
- **序列化**：默认走 v8 序列化（结构化克隆语义的字节化版本，Deno 下已验证）。
  手动序列化只保留给已验证结构化克隆会丢失信息的场景（`error` codec）。
- **分层**：协议层（现有，不动）→ 消息层 `Channel`（现有）→ 帧层 （v8 +
  长度前缀，做成两条 `TransformStream`：对象流 ⇄ 字节流）→ Transport（连接 +
  `openChannel` 多路复用）。新增的只有 "Transport + 帧层 + 适配器"。
- **关键事实**：fork IPC 的 advanced 模式数据可通、但 MessagePort 句柄双向
  `ERR_INVALID_HANDLE_TYPE`；`Deno.Command` 数组 stdio 的"额外 fd"父端没有 JS
  访问器；WebSocket 二进制消息 + v8 帧就是分布式标准形态。
- **跨进程引用**：MessagePort 不可跨进程，因此 ref / iterable / callback 的
  "随消息传输端口"改为"随消息传输建通道指令（令牌）"，这是唯一需要动的协议层
  改动；ref 的 refId / acquire 骨架不变。

## 1. 背景与目标

现状（0.3.x）：

- Actor = Web Worker。`spawn(worker: Worker)` 包装为类型安全代理； 值传输 =
  `postMessage` 结构化克隆；`serveWorker` 运行在 `self` 全局上。
- 引用类值（remote-ref / AsyncIterable / callback）通过 `new MessageChannel()` +
  **MessagePort transfer** 建立独立通道： `core/channel.ts` 的
  `openChannel(ctx)` 把 `port2` 塞进占位符并加入 `ctx.transfer`。
- 只支持同进程 Worker；ref 的跨 worker 路由由主线程引导 （`__acquire-ref` /
  `__serve-ref` / `__ref-acquired`，`spawn.ts`）。

目标：

- **多进程**：用 `Deno.Command` 或 `node:child_process` 拉起 actor 进程。
- **分布式**：actor 跑在不同进程/机器，经 WebSocket / TCP / 任意二进制流互通。
- **约束**：不重新设计 RPC / stream / ref / callback 协议；只换传输层。

## 2. 已验证的事实（探针结论）

| # | 探针                                                                                                 | 结论                                                                                                                                                                                   | 影响                                                                                                                                 |
| - | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| 1 | `node:v8` `serialize/deserialize`（Deno 内）                                                         | Map / Set / BigInt / TypedArray / Date / RegExp / 循环引用全部往返成功                                                                                                                 | v8 序列化可作为统一字节格式，语义与现有结构化克隆 1:1                                                                                |
| 2 | `node:child_process` fork，`stdio: ['ipc','pipe','pipe']` + `serialization: 'advanced'`（Node 原生） | 数据消息带 Map/BigInt/Date 往返成功；MessagePort 作 sendHandle 双向 `ERR_INVALID_HANDLE_TYPE`；`net.Socket` 作 sendHandle 可传                                                         | fork IPC 有 v8 消息通道可用；但**不能传 MessagePort**，只能传内核句柄                                                                |
| 3 | 同探针在 **Deno 运行时内**跑 `node:child_process` fork                                               | 数据往返 OK；MessagePort 句柄同样 `ERR_INVALID_HANDLE_TYPE`（Deno 的 polyfill 与 Node 一致）                                                                                           | 上述结论在 Deno 下同样成立                                                                                                           |
| 4 | `Deno.Command` `stdio: ["piped",...,"piped"]`（N 项）                                                | 数组第 0/1/2 = stdin/stdout/stderr，≥3 = **额外 fd**（经 `kExtraStdio` 传给子进程，源码 `ext/process/40_process.js`）；但父端 JS **只暴露** `.stdin/.stdout/.stderr`，额外 fd 无访问器 | `Deno.Command` 的"额外 fd"父端拿不到流；需要 `node:child_process` 路径（暴露 `ipcPipeRid`/`extraPipeFds`）或直接用 stdin/stdout 承载 |

要点归纳：

- "只有 child_process 支持额外二进制通道"是**误解**。实际是：
  - **fork IPC**（`node:child_process`）自带一条二进制消息通道（advanced = v8
    消息）， 不需要额外 fd；但**句柄传输受限**（MessagePort 不可传）。
  - `Deno.Command` 的额外 fd 是给子进程用的（`/dev/fd/N`），父端没有访问器，
    所以要"父端↔子端"额外通道时它并不顺手。
- 真正决定"能否传引用"的是**句柄/端口 transfer 能力**，不是"是不是二进制通道"：
  MessagePort 通道能 transfer MessagePort；fork IPC 只能传内核句柄；
  字节流（TCP/WebSocket/额外 fd）完全不能传引用。

## 3. 核心架构：Transport（连接）+ 流 + 帧

```
┌──────────────────────────────────────────────────────┐
│ 协议层（现有，不动）                                    │
│   RPC (core/rpc.ts) · stream (core/stream.ts)          │
│   ref / callback codecs · acquire 控制面                │
│   只依赖 Channel.send / onMessage / close               │
├──────────────────────────────────────────────────────┤
│ 消息层（现有 Channel，语义不变，见 3.4）                 │
│   send(value, transfer?) / onMessage / close           │
├──────────────────────────────────────────────────────┤
│ 帧层（新增 core/frame.ts，TransformStream）             │
│   createEncoder(): TransformStream<unknown, Uint8Array> │
│   createDecoder(): TransformStream<Uint8Array, unknown> │
│   v8 序列化 + 4 字节小端长度前缀 + 多路复用（Mux）        │
├──────────────────────────────────────────────────────┤
│ 传输层（新增 core/transport.ts）                        │
│   Transport: send / onMessage / openChannel / onChannel │
│   适配器：MessagePort · fork IPC · stdio · WebSocket ·  │
│           TCP（Deno.connect / WHATWG Streams）           │
└──────────────────────────────────────────────────────┘
```

### 3.1 为什么是 Transport，而不是"MessagePort 单独 API"

统一抽象是"连接"（Transport）：一个 Transport 既承载主消息通道，也负责**新建
逻辑通道**（多路复用）。这与 WebTransport 的形状一致——一条连接 +
`createBidirectionalStream()` / incoming streams。MessagePort 不是特殊 API，
它只是 messageport 型 Transport 的一种实现：它的 `openChannel()` 恰好产出可
transfer 的端口——这是结构化克隆世界里最自然的建连方式。

```ts
// core/transport.ts
interface Transport {
  readonly kind: "messageport" | "framed"; // 帧层/多路复用是否介入
  // 主消息通道（RPC / 控制帧）
  send(frame: unknown, transfer?: Transferable[]): void;
  onMessage(handler: (frame: unknown) => void): void;
  // 新建一条逻辑通道：token 随消息发给对端；对端据 token 重建 Channel
  openChannel(): { channel: Channel; token: unknown };
  // 对端新建的通道（framed 型：Mux 按 channelId 组装后在此投递）
  onChannel(handler: (channel: Channel) => void): void;
  close(): void;
}
```

- **messageport 型**：`openChannel()` = `new MessageChannel()`，`token` =
  `port2`（随消息 transfer）；对端在消息里收到端口即 `connectChannel(port)`。
- **framed 型**：`openChannel()` = 分配 channelId，在主通道发
  `{ __mux: "open", ch }`，`token` = `{ __mux: "open", ch }`（普通可克隆值）；
  对端 Mux 按 channelId 组装子通道并触发 `onChannel`。

codec 的调用点形状不变：原来 `openChannel(ctx)`（建通道 + 把对端身份塞占位符）
上移到 `ctx.transport.openChannel()`；`EncodeContext` 暴露 `transport` 取代直接
操作 `transfer`。"塞端口"只是 messageport 型的特例，不是协议层的分支。

### 3.2 全双工抽象：WHATWG Streams 统一（不提供 Node Duplex 桥接）

JS 现成的主流全双工抽象，按形态分两类：

| 抽象                                                                  | 形态                  | 备注                                                          |
| --------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------- |
| WHATWG Streams（`ReadableStream`/`WritableStream`/`TransformStream`） | 字节/值流，可组合     | Deno 原生；Node 18+；Node 生态需自行转换                      |
| `MessagePort` / `WebSocket` / WebRTC DataChannel                      | 消息型双工            | 自带消息边界，不是字节流                                      |
| **WebTransport**                                                      | **连接 + 多路双向流** | "可新建通道的连接"这一形状的标准化先例（浏览器；Node 实验性） |

结论：内部字节类型统一用 WHATWG `ReadableStream<Uint8Array>` /
`WritableStream<Uint8Array>`；**不提供 Node `stream.Duplex` 桥接**——字节流输入
只接受 WHATWG 形态（`Deno.connect`、进程 stdio 的 Web Stream 天然如此），Node
生态的 `net.Socket` / `Duplex` 需要时由用户侧自行转换。MessagePort / WebSocket
是消息型通道，包成 Transport 时不需要（也不能）转成字节流。

### 3.3 序列化边界用 TransformStream（对象流 ⇄ 字节流）

帧层做成两条标准 TransformStream，用户侧保持对象流（现有 `Remote<T>` 的流
返回类型就是 `AsyncIterable`，`ReadableStream.from(iter)` 与
`stream[Symbol.asyncIterator]()` 免费互转）：

```ts
// core/frame.ts
export function createEncoder(): TransformStream<unknown, Uint8Array>; // 序列化 + 分帧
export function createDecoder(): TransformStream<Uint8Array, unknown>; // 解帧 + 反序列化
```

传输就是一条管道链：`对象流 → encoder → 字节流 → 传输 → 字节流 → decoder → 对象流`。

- **多路复用**放在 decoder 之后：解帧产出 `{ ch, value }`，Mux 按 channelId
  分派到各子通道的队列；子通道即现有 `Channel`。
- **背压**由 TransformStream 的可写侧天然承载；`Channel.send` 的同步语义不变
  （适配器层排队/报错，协议层不感知）。
- **分帧**：默认 4 字节小端长度前缀（uv 约定，批处理友好——WebSocket 一条消息
  可载多帧）；WebSocket 适配器也可选"一帧一消息"。长度前缀统一实现，
  消息型通道不启用。
- 值在序列化前仍走 `PayloadCodecRegistry.encode` 深遍历（占位符替换），
  反序列化后走 `registry.decode` —— 与现有消息通道的编解码流程完全一致。

### 3.4 消息层（现有 Channel）微调

1. `port` 成员可选（framed 子通道没有 MessagePort）。
2. `openChannel(ctx)` 改为 `ctx.transport.openChannel()`（见 3.1）。
3. `Channel.kind` 从传输继承（messageport / framed），供 codec 判断能否塞端口
   （只有 messageport 型能）。

## 4. 序列化策略

- **默认：v8 序列化**。结构化克隆能力表（DESIGN.md 已声明的
  Map/Set/Date/RegExp/TypedArray/BigInt/循环引用…）就是 v8 序列化能力表， 语义
  1:1；Node 的 worker_threads 与 fork IPC 共用同一 v8 实现，跨环境一致。
- **手动序列化保留场景**：`error` codec。结构化克隆会丢自定义子类名/自定义属性
  （DESIGN.md 已验证），error 的 `{name, message, stack}` 手动格式保留；
  其余值一律 v8。
- **不引入 JSON**：fork IPC 默认 JSON 模式丢 Map/Set/BigInt/TypedArray，
  与结构化克隆语义不符；统一 `serialization: 'advanced'`。
- 占位符内嵌的 MessagePort 只在 messageport 型通道有效；framed 型下 codec 不得
  再塞端口，改塞 `transport.openChannel()` 返回的 token（见 3.1）。

## 5. 传输适配器（Transport 实现）与能力矩阵

| Transport 实现                                      | kind        | openChannel 产物                       | 说明                                                                           |
| --------------------------------------------------- | ----------- | -------------------------------------- | ------------------------------------------------------------------------------ |
| MessagePort（Worker 主通道 / link / `__serve-ref`） | messageport | MessagePort（可 transfer）             | 现状；`openChannel` = 新 MessageChannel，端口随消息 transfer                   |
| fork IPC（`node:child_process` advanced）           | framed      | `{ __mux: "open", ch }`（ch 多路复用） | 数据消息可通；MessagePort 句柄 `ERR_INVALID_HANDLE_TYPE`，因此 token 化        |
| Deno 子进程 stdin/stdout                            | framed      | 同上                                   | 兜底方案；父端 `p.stdin/p.stdout`（Web Stream）                                |
| WebSocket（二进制消息）                             | framed      | 同上                                   | 分布式标准形态；`ws.send(bytes)` + `message` 事件                              |
| TCP（`Deno.connect`）                               | framed      | 同上                                   | 本地进程间直连也复用它；字节流输入为 WHATWG Streams（不提供 Node Duplex 桥接） |

- MessagePort 不需要"单独 API"——它就是 messageport 型 Transport（见 3.1）。
- framed 型的 `openChannel` 走既有连接多路复用，不新建 OS 连接；主↔子进程、
  子↔子、分布式共用同一套 Mux 代码。

## 6. 跨进程引用语义（唯一必须动的协议层改动）

现状（同进程）：codec `encode` 时 `openChannel()` 新建 MessageChannel，把
`port2` 塞进占位符 + transfer；`__serve-ref` / `__ref-acquired` 把端口送进两端，
通道建立是"随消息移走一个端口"。

跨进程问题：fork IPC 不能传 MessagePort；字节流通道不能传任何句柄。

结论（与 3.1 一致）：**通道身份不再随消息走，改为"建通道指令（token）"随消息
走**。`openChannel()` 是 Transport 的一级操作，token 是它的产物——对 messageport
型是端口，对 framed 型是 `{ __mux: "open", ch }`。

- **主↔子进程**：framed 型在既有 IPC 上多路复用（Mux 按 `ch` 头分派），
  省掉每流一条连接；对 RPC/stream codec 透明。
- **子↔子 / 分布式**：新连接（TCP 回连 / WebSocket），地址由引导层提供；
  本地多进程即 localhost TCP，与分布式复用同一套建连代码。
- ref 的 refId（ownerId 前缀）与 `__acquire-ref` 主线程路由骨架**不动**；
  变化的是 `__serve-ref` 的交付物：从"一个端口"变成"一条建连指令（token）"。
- iterable / callback 同理：`startStreamProducer` / `createRemoteIterable` /
  callback 的 per-value 通道改为 per-value 令牌 + 按需建立。
- 引导层（主进程 / 协调者）仍是"一次性路由"：定位 owner → 触发建连；
  建立完成后数据路径绕过引导层（与现有语义一致）。

> 这条改动的唯一目的：把"通道身份"从"可被 transfer 的对象"抽象成"可寻址的
> 令牌"。传输层统一之后，Worker / 进程 / 分布式共享同一套 codec 与协议。

## 7. 实现步骤（分阶段）

### Phase 0 — 探针固化（已完成）

见第 2 节。结论已用 2026-08 实测验证，后续实现以此为事实依据。

### Phase 1 — Transport + 帧层（TransformStream）+ 适配器 ✅ 已完成

- `core/transport.ts`：`Transport` 接口（`openChannel` / `onChannel`）； 适配器
  `fromMessagePort` / `fromNodeIpc` / `fromWebSocket`。字节流输入统一为 WHATWG
  `ReadableStream`/`WritableStream<Uint8Array>`， **不提供 Node `stream.Duplex`
  桥接**。
- `core/frame.ts`：`createEncoder` / `createDecoder`（TransformStream， 对象流 ⇄
  字节流）；Mux（按 channelId 分派子通道）。
- `core/channel.ts`：`Channel.kind` 标注；`port` 可选；`openChannel(ctx)` 改为
  `ctx.transport.openChannel()`；`connectToken`（Mux 通道令牌关联）。
- 验证：帧层单测（分段输入、坏帧、空载荷、大载荷、多路复用交错）+ transport
  单测（messageport / framed / message 三形态）。

### Phase 2 — 进程 actor（多进程）✅ 已完成（口径有更新）

- `spawn.ts`：`spawn` 泛化为接收 `Worker | Transport`（Worker 经
  `fromMessagePort` 转 Transport 后递归）；新增 `spawnProcess`（fork IPC + Deno
  权限控制）和 `spawnNode`（多 actor 节点）作为便捷封装。**`spawn*` 系列
  不是独立入口，`spawn(Transport)` 是统一入口**。
- `worker_runtime.ts`：`createRuntime` 共享（`serveWorker` / `serveProcess` /
  `serveNode` 多 actor）。
- **进程通道用 `node:child_process` fork IPC（`fromNodeIpc`）**，不是
  stdin/stdout——fork IPC 是带外通道，子进程 stdout 日志不污染协议。
- `pool.ts`：`spawnWorker` 工厂仍 Worker-only（进程 pool 未做）。
- 验证：进程版 RPC / AsyncIterable / AbortSignal / callback 端到端；多 actor
  节点端到端；混合拓扑未做。

### Phase 3 — 分布式（WebSocket）部分完成

- `fromWebSocket`（消息型 Transport，Blob→v8 反序列化）✅ 本地回环端到端。
- **TCP 适配器明确不做**（WebSocket 取代；TCP 仅作为底层可选，用户侧自行桥接）。
- 引导层（地址注册/发现：谁在哪、如何回连）❌ 未做。
- acquire / ref / iterable 跨连接（令牌化已就绪）：iterable / abort-signal /
  callback / remote-ref 新鲜令牌已跨进程；**remote-ref 的 refId 间接共享
  （refId-only 转发 + acquire 引导通道）尚未跨进程端到端验证** ❌。
- 验证：本机 WS 端到端 ✅；多机 ❌。

### Phase 4 — 握手与协议版本 ✅ 已完成

- 握手帧已携带传输能力字段（`kind`: "messageport" / "framed" / "message"），
  spawn 对两端 kind 做一致性校验（Mux 传输不得被当作 messageport 使用）。
  `PROTOCOL_VERSION` 暂未 bump（kind 字段向后兼容缺失方，视为 messageport）。

## 8. 需要改动的文件清单

| 文件                                              | 改动                                                                           | 阶段                            |
| ------------------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------- |
| `core/transport.ts`（新）                         | `Transport` 接口（`openChannel`/`onChannel`）+ 适配器                          | ✅ P1                           |
| `core/frame.ts`（新）                             | `createEncoder`/`createDecoder`（TransformStream）+ Mux                        | ✅ P1                           |
| `core/channel.ts`                                 | `Channel.kind` 标注；`port` 可选；`openChannel` 改走 transport；`connectToken` | ✅ P1                           |
| `core/codec.ts`                                   | `EncodeContext`/`DecodeContext` 暴露 `transport`；token 语义                   | ✅ P1                           |
| `spawn.ts`                                        | `spawn` 泛化（Worker \| Transport）+ `spawnProcess` + `spawnNode`              | ✅ P2                           |
| `worker_runtime.ts`                               | `createRuntime` 共享 + `serveProcess` + `serveNode`                            | ✅ P2                           |
| `core/stream.ts` + iterable/abort/callback codecs | 通道令牌化（per-value 通道改为令牌 + 按需建立）                                | ✅ P2/P3                        |
| `mod.ts` / `codec.ts`                             | 公共导出（spawn 泛化 + Transport 适配器 + serveNode 等）                       | ✅ P3                           |
| `examples/remote_ref/ref_codec.ts`                | remote-ref 令牌化（跨进程）                                                    | ✅ P3                           |
| `core/worker-context.ts`                          | `triggerAcquire` 传输侧判定                                                    | ✅ P3                           |
| `pool.ts`                                         | 工厂类型放宽（`Worker \| Transport`）/ `createProcessPool`                     | ✅ P3 / ❌ 待做                 |
| 引导层（新）                                      | 地址注册/发现（谁在哪、如何回连）                                              | ❌ 待做                         |
| `core/protocol.ts`                                | 握手传输能力字段；版本 bump                                                    | ✅ P4（kind 字段；版本未 bump） |
| 示例                                              | 进程版 calculator / 混合拓扑                                                   | ✅ P2 进程示例 / ❌ 混合拓扑    |
| `DESIGN.md`                                       | 传输章节重写为本文第 3–6 节口径                                                | ✅ 已同步                       |

## 9. 兼容性与风险

- **向后兼容**：Phase 1 纯新增，0.3.x 行为不变；Phase 2 保留 `spawn(worker)`
  签名，`spawnProcess` 是新增入口；Worker 路径的 MessagePort transfer 语义不变。
- **风险与对策**：
  - fork IPC advanced 在 Deno 的成熟度：探针显示数据路径可用、句柄不可传——
    我们不依赖句柄，风险已被架构规避。
  - `Deno.Command` 额外 fd 父端访问未完全验证：Phase 2 先用 stdin/stdout 兜底，
    避免阻塞主路径。
  - 分布式"回连"需要可达地址（NAT / 防火墙）：引导层设计时记录该限制，
    不承诺穿透。
  - 帧层大数据包：长度前缀 UInt32LE（上限 4 GiB），`maxFrame` 防异常输入；
    流式解帧注意内存。
  - 背压：framed 型的背压由 `createEncoder`/`createDecoder` 的 TransformStream
    承载；`Channel.send` 的同步语义不变（适配器层排队/报错，协议层不感知）。

## 10. 未决问题

- `Deno.Command` 额外 fd 的父端访问最终方案（op 层 / `node:child_process` 路径 /
  放弃，只用 stdin/stdout）。
- 分布式引导层形态：共享注册表？主进程登记？各节点自发现？
- 字节流通道上 callback 的按需连接实现（callback 是轻量单向通道，令牌化成本）。
- 混合拓扑（一端 Worker、一端进程、一端远端）的 ref 恢复语义是否一致
  （应一致——通道身份已令牌化，owner 只认 refId）。
- remote-ref 的 refId 间接共享（refId-only 转发）跨进程验证（token 引导通道）。
