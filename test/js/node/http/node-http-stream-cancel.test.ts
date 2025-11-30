/**
 * Test for Node.js http server with ReadableStream cancel on client disconnect
 *
 * This test verifies that when a client disconnects from a Node.js http server
 * that is streaming a Response with a ReadableStream body, the stream's cancel()
 * method is properly called.
 *
 * Related issue: https://github.com/oven-sh/bun/issues/[TBD]
 * SvelteKit with node-adapter uses this pattern for SSE and other streaming responses.
 */

import { createTest } from "node-harness";
import { createServer } from "node:http";
import { connect } from "node:net";

const { describe, expect, test } = createTest(import.meta.path);

function listen(server: any): Promise<URL> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject("Timed out"), 5000).unref();
    server.listen({ port: 0 }, (err: any, hostname: string, port: number) => {
      clearTimeout(timeout);

      if (err) {
        reject(err);
      } else {
        resolve(new URL(`http://${hostname}:${port}`));
      }
    });
  });
}

describe("node:http ReadableStream cancel on disconnect", () => {
  test("should call ReadableStream cancel() when client disconnects", async () => {
    let cancelCalled = false;
    let streamStarted = false;
    let closeEventFired = false;
    const cancelPromise = new Promise<void>(resolve => {
      const server = createServer((req, res) => {
        // Create a ReadableStream similar to SvelteKit SSE pattern
        const stream = new ReadableStream({
          start(controller) {
            streamStarted = true;
            // Keep stream open - don't enqueue or close yet
            // This simulates a long-lived SSE or streaming response
          },
          cancel(reason) {
            console.log("Stream cancelled:", reason);
            cancelCalled = true;
            resolve();
          },
        });

        // Create a Response with the stream
        const response = new Response(stream, {
          headers: {
            "Content-Type": "text/event-stream",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
          },
        });

        // Write the Response to the Node.js ServerResponse
        res.writeHead(response.status, {
          "Content-Type": response.headers.get("Content-Type") || "text/event-stream",
          "Connection": "keep-alive",
          "Cache-Control": "no-cache",
        });

        // Stream the body to the response
        if (response.body) {
          const reader = response.body.getReader();

          // THIS IS THE KEY: Wire up close/error events to cancel the reader
          // This is what SvelteKit does in handler.js:1146-1157
          const cancel = (error?: Error) => {
            console.log("Close/error event fired, cancelling reader");
            closeEventFired = true;
            res.off("close", cancel);
            res.off("error", cancel);
            reader.cancel(error).catch(() => {});
            if (error) res.destroy(error);
          };

          res.on("close", cancel);
          res.on("error", cancel);

          const pump = async () => {
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                if (!res.write(value)) {
                  res.once("drain", pump);
                  return;
                }
              }
              res.end();
            } catch (err) {
              console.error("Stream error:", err);
              cancel(err instanceof Error ? err : new Error(String(err)));
            }
          };

          pump();
        } else {
          res.end();
        }
      });

      listen(server).then(url => {
        // Connect and immediately disconnect
        const socket = connect({
          host: url.hostname,
          port: parseInt(url.port),
        });

        socket.on("connect", () => {
          // Send HTTP request
          socket.write("GET / HTTP/1.1\r\n");
          socket.write(`Host: ${url.hostname}:${url.port}\r\n`);
          socket.write("\r\n");

          // Wait a bit for the stream to start, then disconnect
          setTimeout(() => {
            socket.destroy();

            // Give some time for cancel to be called
            setTimeout(() => {
              server.close();
              if (!cancelCalled) {
                resolve(); // Resolve anyway to finish the test
              }
            }, 100);
          }, 50);
        });

        socket.on("error", err => {
          console.error("Socket error:", err);
          server.close();
          resolve();
        });
      });
    });

    await cancelPromise;

    expect(streamStarted).toBe(true);
    expect(closeEventFired).toBe(true);
    expect(cancelCalled).toBe(true);
  }, 10000);

  test("should call cancel() with fetch client abort", async () => {
    let cancelCalled = false;
    let streamStarted = false;

    const server = createServer((req, res) => {
      const stream = new ReadableStream({
        start(controller) {
          streamStarted = true;
          // Keep stream open
        },
        cancel(reason) {
          console.log("Stream cancelled (fetch abort):", reason);
          cancelCalled = true;
        },
      });

      const response = new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Connection": "keep-alive",
        },
      });

      res.writeHead(response.status, {
        "Content-Type": response.headers.get("Content-Type") || "text/event-stream",
        "Connection": "keep-alive",
      });

      if (response.body) {
        const reader = response.body.getReader();

        // Wire up close/error events to cancel the reader (SvelteKit pattern)
        const cancel = (error?: Error) => {
          res.off("close", cancel);
          res.off("error", cancel);
          reader.cancel(error).catch(() => {});
          if (error) res.destroy(error);
        };

        res.on("close", cancel);
        res.on("error", cancel);

        const pump = async () => {
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              res.write(value);
            }
            res.end();
          } catch (err) {
            res.end();
          }
        };

        pump();
      } else {
        res.end();
      }
    });

    const url = await listen(server);

    try {
      const controller = new AbortController();

      // Start the fetch
      const fetchPromise = fetch(url, { signal: controller.signal });

      // Abort after a short delay
      await new Promise(resolve => setTimeout(resolve, 50));
      controller.abort();

      try {
        await fetchPromise;
      } catch (err) {
        // Expected to throw due to abort
      }

      // Wait for cancel to be called
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(streamStarted).toBe(true);
      expect(cancelCalled).toBe(true);
    } finally {
      server.close();
    }
  }, 10000);

  test("should handle stream cancel with chunked encoding", async () => {
    let cancelCalled = false;
    let chunksWritten = 0;

    const server = createServer((req, res) => {
      const stream = new ReadableStream({
        async start(controller) {
          // Send a few chunks before client disconnects
          for (let i = 0; i < 5; i++) {
            controller.enqueue(new TextEncoder().encode(`data: chunk ${i}\n\n`));
            chunksWritten++;
            await new Promise(resolve => setTimeout(resolve, 10));
          }
        },
        cancel(reason) {
          console.log("Stream cancelled (chunked):", reason);
          cancelCalled = true;
        },
      });

      const response = new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Transfer-Encoding": "chunked",
        },
      });

      res.writeHead(response.status, {
        "Content-Type": response.headers.get("Content-Type") || "text/event-stream",
        "Connection": "keep-alive",
      });

      if (response.body) {
        const reader = response.body.getReader();

        // Wire up close/error events to cancel the reader (SvelteKit pattern)
        const cancel = (error?: Error) => {
          res.off("close", cancel);
          res.off("error", cancel);
          reader.cancel(error).catch(() => {});
          if (error) res.destroy(error);
        };

        res.on("close", cancel);
        res.on("error", cancel);

        const pump = async () => {
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              if (!res.destroyed) {
                res.write(value);
              }
            }
            if (!res.destroyed) {
              res.end();
            }
          } catch (err) {
            if (!res.destroyed) {
              res.end();
            }
          }
        };

        pump();
      } else {
        res.end();
      }
    });

    const url = await listen(server);

    try {
      const socket = connect({
        host: url.hostname,
        port: parseInt(url.port),
      });

      await new Promise<void>((resolve, reject) => {
        socket.on("connect", () => {
          socket.write("GET / HTTP/1.1\r\n");
          socket.write(`Host: ${url.hostname}:${url.port}\r\n`);
          socket.write("\r\n");

          // Wait for some data, then disconnect
          socket.on("data", () => {
            setTimeout(() => {
              socket.destroy();
              resolve();
            }, 50);
          });
        });

        socket.on("error", reject);
      });

      // Wait for cancel to be called
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(chunksWritten).toBeGreaterThan(0);
      expect(cancelCalled).toBe(true);
    } finally {
      server.close();
    }
  }, 10000);
});
