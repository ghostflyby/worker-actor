/** Test fixture: a worker module that never calls serveWorker() — no handshake ever arrives. */
export const rpc = {
  ping(): string {
    return "pong";
  },
};
