// UDP Relay Server for Deno Deploy
// Deploy: https://dash.deno.com/new

const UDP_TIMEOUT = 60000; // 60 seconds

interface UDPConnection {
  socket: Deno.DatagramConn;
  lastActivity: number;
}

const connections = new Map<string, UDPConnection>();

// Cleanup old connections
setInterval(() => {
  const now = Date.now();
  for (const [key, conn] of connections.entries()) {
    if (now - conn.lastActivity > UDP_TIMEOUT) {
      try {
        conn.socket.close();
      } catch (e) {
        console.error(`Failed to close socket: ${e}`);
      }
      connections.delete(key);
      console.log(`Cleaned up connection: ${key}`);
    }
  }
}, 30000); // Check every 30 seconds

Deno.serve({ port: 7300 }, async (req) => {
  if (req.headers.get("upgrade") !== "websocket") {
    return new Response("UDP Relay Server - WebSocket required", { 
      status: 426,
      headers: { "Upgrade": "websocket" }
    });
  }

  const { socket, response } = Deno.upgradeWebSocket(req);

  socket.addEventListener("open", () => {
    console.log("Client connected");
  });

  socket.addEventListener("message", async (event) => {
    try {
      const data = new Uint8Array(event.data);
      const separator = data.indexOf(0x7c); // "|" separator
      
      if (separator === -1) {
        socket.send("Error: Invalid format");
        return;
      }

      const header = new TextDecoder().decode(data.slice(0, separator));
      const payload = data.slice(separator + 1);

      const [protocol, targetHost, targetPort] = header.split(":");
      
      if (protocol !== "udp") {
        socket.send("Error: Only UDP supported");
        return;
      }

      const connKey = `${targetHost}:${targetPort}`;
      let conn = connections.get(connKey);

      if (!conn) {
        try {
          const udpConn = Deno.listenDatagram({
            port: 0,
            transport: "udp",
            hostname: "0.0.0.0"
          });

          conn = {
            socket: udpConn,
            lastActivity: Date.now()
          };
          connections.set(connKey, conn);

          console.log(`New UDP connection: ${connKey}`);

          // Listen for responses
          (async () => {
            try {
              for await (const [data] of udpConn) {
                if (socket.readyState === WebSocket.OPEN) {
                  socket.send(data);
                  conn!.lastActivity = Date.now();
                }
              }
            } catch (e) {
              console.error(`UDP receive error: ${e}`);
            }
          })();
        } catch (e) {
          socket.send(`Error creating UDP socket: ${e}`);
          return;
        }
      }

      conn.lastActivity = Date.now();

      // Send UDP packet
      try {
        await conn.socket.send(payload, {
          transport: "udp",
          hostname: targetHost,
          port: parseInt(targetPort)
        });
        console.log(`Sent ${payload.length} bytes to ${connKey}`);
      } catch (e) {
        socket.send(`Error sending UDP: ${e}`);
      }
    } catch (e) {
      console.error(`Message handling error: ${e}`);
      socket.send(`Error: ${e}`);
    }
  });

  socket.addEventListener("close", () => {
    console.log("Client disconnected");
  });

  socket.addEventListener("error", (e) => {
    console.error("WebSocket error:", e);
  });

  return response;
});

console.log("UDP Relay Server running on port 7300");
console.log("WebSocket endpoint: ws://localhost:7300");

