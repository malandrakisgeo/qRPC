## Generated with Claude Opus 4.6 


# qRPC — Quick Remote Procedure Call

Custom Layer 2.5 protocol for ultra-low-latency RPC over raw Ethernet frames.
Bypasses network and transport layers entirely.

## Building

```bash
make all      # builds server, client, and tests
make test     # runs 30 unit tests (no root needed)
make clean
```

Requires: Linux, g++ (C++17), `CAP_NET_RAW` for server/client (root).

## Wire Format

The implementation follows the spec structs exactly:

| Struct            | Used for                                            | Layout (after ETH header)                                          |
|-------------------|-----------------------------------------------------|--------------------------------------------------------------------|
| `init_req`        | SESSION_INIT (0x0101)                               | magic(2) + opcode(2) + **qRPC_init**(4) + sid(2) + rem(2) + dlen(2) + data + checksum? |
| `init_req`        | RESEND_FRAMES (0x2020)                              | magic(2) + opcode(2) + sid(2) + rem(2) + dlen(2) + missing_frames[uint16] + checksum?   |
| `sequel_frame`    | SEQUEL_REQ (0x0202), TRIGGER_RESP (0x0707), DATA_RESPONSE (0x0811) | magic(2) + **sid(2)** + opcode(2) + rem(2) + dlen(2) + data + checksum? |
| `session_generic` | ACK, ERROR_INFO, COMPLETION, CANCELLATION, DATA_RESP_INIT, MEMORY_OVERFLOW | magic(2) + **sid(2)** + opcode(2) + num(2) + message_code(2) + checksum? |
| broadcast         | PERIODIC_BROADCAST (0x9999)                         | 3× (magic(2) + opcode(2)) = 12 bytes                              |

Note: `init_req` puts opcode at [2..3], while `sequel_frame`/`session_generic` put session_id there.
Checksum is 1 byte (XOR of all data bytes), present only in slow mode.

## Spec Discrepancies Noted

1. **`remaining_frames`**: note 5 says "unsigned char" but struct uses `uint16` → we use `uint16`
2. **`proc_id`**: spec says "up to 256" but struct uses `uint16` → we use `uint16`
3. **Checksum size**: intro says "two bytes" but struct says `char` (1 byte) → we use 1 byte
4. **Trigger response opcode**: description says 0x0808 but opcode table says 0x0707 → we use 0x0707

## Registering Procedures

The server is **type-oblivious** — it only stores `ProcedureFunc` (bytes → ProcResult).
Register typed functions via `wrap_procedure()`:

```cpp
#include "qrpc_server.h"
#include "qrpc_marshal.h"

uint32_t add(uint32_t a, uint32_t b) { return a + b; }
void fire() { /* side effect */ }
std::string rev(std::string s) { return {s.rbegin(), s.rend()}; }

int main() {
    qrpc::Server server("eth0");
    server.register_procedure(0x01, qrpc::wrap_procedure(add));
    server.register_procedure(0x02, qrpc::wrap_procedure(fire));
    server.register_procedure(0x03, qrpc::wrap_procedure(rev));
    // Thread-safe: register/unregister at any time
    server.start();
    pause();
}
```

Client side:
```cpp
auto args = qrpc::serialize_args(uint32_t(17), uint32_t(25));
auto res  = client.trigger(server_mac, 0x01, args);
uint32_t sum = qrpc::deserialize_result<uint32_t>(res);
```

Supported types: `uint8/16/32/64`, `int8/16/32/64`, `float`, `double`, `bool`, `std::string`, `std::vector<uint8_t>`.

## Key Implementation Details

- **Pre-allocated FrameStore** (uint16-keyed): on data-exchange init, the server pre-allocates
  slots for the declared number of frames. `bad_alloc` → MEMORY_OVERFLOW (0x3232).
- **Missing-frame detection**: housekeeping thread checks every 0.5ms. Idle ≥3ms → sends
  RESEND_FRAMES (0x2020) with uint16 list of missing countdown numbers. Client stores all
  sent frames and resends on request.
- **Client-side RESEND_FRAMES**: when receiving multi-frame responses, the client detects gaps
  after 3ms idle and sends RESEND_FRAMES to the server.
- **Session reaper**: sessions exceeding 30s are cleaned up.
- **Response retransmission**: exponential backoff (0.1s → 10s), then termination.
- **Duplicate suppression**: both sides ACK duplicate frames silently (edge-subcase-XX).

## Project Structure

```
include/
  qrpc_protocol.h   — Wire structs, constants, parser, builders, checksum
  qrpc_socket.h     — AF_PACKET raw socket wrapper
  qrpc_server.h     — FrameStore, session state machine, Server class
  qrpc_client.h     — Client class
  qrpc_marshal.h    — Type-safe serialization, wrap_procedure(), BufferReader/Writer

src/
  qrpc_socket.cpp   — Socket implementation
  qrpc_server.cpp   — RX loop, housekeeping, all handlers
  qrpc_client.cpp   — Discover, trigger, data-exchange, RESEND handling
  server_main.cpp   — Demo: 5 typed procedures registered via wrap_procedure
  client_main.cpp   — Demo: exercises all operations with typed args
  test_protocol.cpp — 30 unit tests (wire format, FrameStore, marshal)
```

## Running
If running on a single machine:
```
sudo ip link add veth0 type veth peer name veth1
sudo ip link set veth0 up
sudo ip link set veth1 up
```
Then run server on veth0 and client on veth1.
