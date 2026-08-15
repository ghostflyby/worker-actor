# Worker ⇄ Actor 设计

把 Web Worker 包装成**类型安全 Actor**
的最小实现。设计目标：静态类型、深层嵌套对象传递、
自动建立消息通道、自动建立传输。零运行时依赖（仅测试用 `@std/assert`）。

## 核心洞察

Worker 与 Actor 是同一个模型：

| Actor 模型             | Web Worker                        |
| ---------------------- | --------------------------------- |
| 有地址、可投递消息     | `worker.postMessage()` 有稳定地址 |
| 邮箱按序处理消息       | worker 事件循环单线程按序处理     |
| 消息不可共享、按值传递 | structured clone 按值复制         |
| 异常不跨地址传播       | 错误需显式序列化回传              |
| 生命周期受外部控制     | `terminate()` / `close()`         |

因此只需要四件事：**消息通道**（postMessage + 帧协议）、**代理**（Proxy + 自增
id 关联请求/响应）、 **传输**（structured
clone，原生支持深层嵌套对象）、**类型**（TS 类型级代理推导）。

## 项目结构

```
mod.ts                    # 公共出口
core/protocol.ts          # 帧类型、错误序列化、握手版本
core/codec.ts             # 通用 Codec 接口 + PayloadCodecRegistry（深遍历/占位符/生命周期）
core/codecs/              # 内置 codec：iterable / error / abort-signal
core/stream.ts            # 流通道原语（MessageChannel 泵取/重建/背压/cancel）
spawn.ts                  # 主线程侧：spawn() 生成类型安全 Proxy + 生命周期 + codec 校验
worker_runtime.ts         # worker 侧：serveWorker(api) 注册 RPC、握手、派发
examples/calculator/      # worker.ts + main.ts 端到端示例
test_fixtures/            # codec 机制测试专用 worker
main_test.ts / codec_test.ts  # 真实 Worker 集成测试
```

## 用法

```ts
// worker.ts —— rpc 对象就是 Actor 的 API 面，导出以便主线程引用类型
import { serveWorker } from "../worker_runtime.ts";

export const rpc = {
  add(a: number, b: number): number {
    return a + b;
  },
  async report(): Promise<object> {
    /* 可返回深层嵌套 + Map/Set/Date/TypedArray */
  },
};

serveWorker(rpc);

// main.ts —— import type 只取类型，不执行 worker 模块副作用
import type * as WorkerModule from "./worker.ts";
import { spawn } from "../spawn.ts";

const actor = await spawn<typeof WorkerModule.rpc>(
  new Worker(import.meta.resolve("./worker.ts"), { type: "module" }),
);
const sum = await actor.add(1, 2); // sum: number，编译期校验
await actor.dispose();
```

## 类型设计（静态类型核心）

```ts
export type Remote<T> = {
  [K in keyof T]: T[K] extends RpcFn
    ? (...args: Parameters<T[K]>) => Promise<Resolved<ReturnType<T[K]>>>
    : never;
};
```

- 只保留函数成员，非函数成员编译为 `never`（写错字段立刻报错）。
- 返回值统一 Promise 化；类型单参
  `Promise`（`Awaited`）自动拍平，不依赖递归类型。
- 调用方永远写 `spawn<typeof WorkerModule.rpc>`，不写第二份接口定义——worker
  即类型真相。
- 代理额外挂 `dispose(): Promise<void>` 生命周期方法。

## 传输设计（深层嵌套对象 + AsyncIterable + Codec 机制）

**常规值**依赖 postMessage 的 **structured clone**，不做自定义序列化：

- 原生支持：任意深度嵌套对象/数组、`Map`、`Set`、`Date`、`RegExp`、
  `TypedArray`、`ArrayBuffer`、`Error` 内置族与 `DOMException`（内置子类型
  身份、name/message/stack 保真）、`BigInt`、`Promise`（自动转 MessagePort）。
- 天然**按值复制**，无共享状态，符合 Actor 消息语义。
- 缺点（若将来需要再处理）：自定义类实例原型链退化、循环引用变空对象、无压缩。

**不能可靠克隆的值**由**通用 Codec 注册表**（`core/codec.ts`）接管——这是
"自定义传输"的扩展点，不是逐个 if 分支硬编码：

```ts
interface Codec<T> {
  readonly tag: string; // wire 占位符 id，decode 按 tag 查表
  matches(value: unknown): value is T;
  encode(value: T, ctx): unknown; // → { __wCodec: tag, ... }，可建通道并转移端口
  decode(placeholder, ctx): T; // 重建原类型
  onRegistryFail?(state): void; // actor 终止/崩溃时清理资源
}
```

- `spawn(worker, { codecs })` 与 `serveWorker(api, { codecs })` 两侧注册，用户
  codec 先于内置匹配（可覆盖同名 tag）；注册表深遍历载荷（对象/数组/Map/Set
  嵌套任意层），按注册顺序取首个 matches 命中的 codec。
- **握手携带两侧 codec tag 清单**并校验：不一致时 spawn 直接 reject 并报出
  缺失/多余项——注册不匹配从"静默产出垃圾"变成启动即失败。
- decode 遇未知占位符 tag 抛错（loud fail）。
- 内置三个 codec：
  - `iterable`：AsyncIterable/同步 Iterable/状态化 Iterator 走 MessageChannel
    专属通道（见下），含惰性/背压/cancel/错误回传/死亡关闭语义。
  - `error`：载荷内**自定义 Error 子类**手动序列化（内置 Error
    族原生保真不接管）。 默认只留
    name/message/stack/cause；`createErrorCodec({ keepOwnProperties: true })`
    额外保留枚举自有属性。decode 还原为 RemoteError（name 保留自定义子类名）。
  - `abort-signal`：AbortSignal 走通道桥接（实测 + 规范核实：structured clone
    既不支持 AbortSignal 序列化，Deno 实现也不保留原型与 aborted 状态）。已
    abort 的源信号立即发 status 帧；未 abort 时监听 abort 事件转发；actor
    死亡时对端 重建信号一并 abort。状态异步生效（一个 tick
    内），事件驱动的消费方不受影响。
- 状态按 codec 实例隔离（注册表持有 per-codec 状态槽）：一个 actor 的死亡
  不会误关另一个 actor 的流。

**AsyncIterable 不能结构化克隆**（携带闭包与运行状态），iterable codec 为每条流
**手动 `new MessageChannel()`** 建立专属通道（`core/stream.ts` 通道原语）：

- 发送侧持有 `port1` 把元素泵进通道，接收侧拿到随消息**转移**的 `port2`，
  **重建**一个本地 AsyncIterable 按需拉取；通道与 RPC 主通道分离，流的洪峰
  不会阻塞请求/响应。
- 流通道协议：`start`（首次 `next()` 惰性启动，未迭代不消耗生产者）→
  `item`/`done`/`error`（生产者→消费者）→ `cancel`（提前停止，触发对端 生成器
  `finally` 清理）。
- 生产者的中间异常序列化回传，主线程还原为 `RemoteError`； actor 终止/崩溃时
  `failAll()` 统一关闭所有在途流。
- 无头/背压：`item` 只在对端 `next()` 挂起时投递，生成器在两条消息之间
  await，天然背压，不会把对端邮箱塞爆。

**ReadableStream 的结论**：按规范可结构化克隆，但语义是"一次性准转移"——
只能在未读、未锁定前 clone 一次，克隆后原流被扰动。需要一次性交给对端消费
就免费用它；要保留原流继续用或挂钩生命周期，用 `rs[Symbol.asyncIterator]()` 走
iterable 通道。

**函数/闭包不 codec 化**：违背结构化克隆语义；Remote\<T\> 已把函数当 RPC 方法
处理，不当数据传输。

## 协议设计

```ts
type Frame =
  | { type: "handshake"; version: number; codecs: string[] } // worker 就绪 + codec 清单
  | { type: "request"; id: number; method: string; args: unknown[] }
  | { type: "response"; id: number; ok: true; value: unknown }
  | { type: "response"; id: number; ok: false; error: SerializedError }
  | { type: "dispose" }; // 优雅关闭
```

- **握手帧**让 `spawn()` 等 worker 模块加载 + `serveWorker()` 完成后再 resolve；
  携带两侧 codec tag 清单，注册不一致直接判死（超时默认 10s，可配）。 版本/codec
  不匹配或 worker 崩溃都会 reject 握手，spawn() 不会挂起。
- **自增 id** 关联请求/响应，响应允许乱序到达；worker 单线程天然串行处理请求，
  与 Actor"同一 Actor 按序处理消息"语义一致。并发调用由 pending Map 各自路由。
- **错误序列化**：现代运行时可以结构化克隆 Error 族与 DOMException（内置子类型
  身份、name/message/stack 保真），但自定义属性丢失、自定义 Error 子类会退化为
  Error（instanceof 失效、name 错乱）。协议层因此统一序列化为
  `{name, message, stack}`， 主线程还原为 `RemoteError`（instanceof Error，保留
  worker 侧 name/stack）， 行为跨实现一致、可扩展 cause/code 等字段；与"Actor
  异常不回传"形成对比—— 这里显式回传。
- **死亡检测**：`onerror` / `onmessageerror` / 握手超时 / dispose 都会进入死态，
  in-flight 调用全部 reject `ActorDiedError`，之后的调用立即拒绝。

## 生命周期

| 事件                   | 行为                                                                                  |
| ---------------------- | ------------------------------------------------------------------------------------- |
| `actor.dispose()`      | 发 dispose 帧 → worker `self.close()`，随后 `terminate()` 兜底；in-flight 全部 reject |
| worker 崩溃（onerror） | 死态；in-flight reject；后续调用抛 `ActorDiedError`                                   |
| 握手超时               | 死态，reject 并提示"did it call serveWorker()?"                                       |
| `Symbol.dispose`       | 支持 `using actor = ...`（TS 5.2+）                                                   |

## 已知边界

- worker 模块只允许一个 RPC 入口对象（可改：`serveWorker({ns: {…}})`
  嵌套命名空间）。
- 若 `worker.postMessage` 抛 `DataCloneError`（如传了函数/类实例），reject
  并进入死态。
- 流元素本身要求可结构化克隆（iterable 里嵌套 iterable
  会各自建新通道，正确但不常见）。
- 同步可迭代对象（生成器、自定义 Iterable）会包装为异步可迭代后走同一通道；
  数组/Map/Set 等原生容器视为普通值直接克隆，不建通道。
- 消费者不 `return()` 也不拉完的流在 actor 存活期间会保持挂起（通道开着）。
- 浏览器端部署需换用
  `new Worker(new URL("./worker.ts", import.meta.url), { type: "module" })` 的
  `import.meta.resolve` 替代写法（browser 无 `import.meta.resolve`）。
- 非函数字段、泛型重载等边界类型在 `Remote<T>` 下会得到 `never`/宽松类型，
  如需严格化可引入 `satisfies` 契约类型（见下）。

## 后续演进

- **命名空间/事件**：`{ type: "event"; name; payload }` 帧 + 订阅/退订，事件按
  `[name]` 分类投递。
- **池化**：多个 worker + 抢占调度，spawn 之上包
  `createActorPool<typeof rpc>(n, url)`。
- **上传**：`Transferable` 列表参数（ArrayBuffer/OffscreenCanvas 零拷贝），
  类型上用 `Transfer<ArrayBuffer>` 标记。
- **双向**：worker 反向调用主线程 API，对称实现一套 runtime（MessagePort
  复用同一协议）。
- **版本协商**：handshake 里 `version` 改为支持的最低版本列表。
- **契约校验**：`spawn(satisfies<Contract> …)` 校验 worker API
  与契约形状一致，失败即 throw。
- **复用通道**：一个 worker 注册多个 API 面，spawn
  返回子命名空间代理，共享同一帧通道。
- **推送事件**：把"拉取式"的 AsyncIterable 流再包一层，对端推送数据时自动投递
  到本地事件订阅（拉/推双模）。
