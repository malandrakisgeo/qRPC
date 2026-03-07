#include "qrpc_client.h"
#include "qrpc_marshal.h"
#include <cstdio>

int main(int argc, char* argv[]) {
    const char* iface = (argc > 1) ? argv[1] : "eth0";
    fprintf(stderr, "=== qRPC client ===\nInterface: %s\n\n", iface);

    qrpc::Client client(iface);

    fprintf(stderr, "Discovering server...\n");
    qrpc::MacAddr server;
    try { server = client.discover_server(10'000'000); }
    catch (const std::exception& e) { fprintf(stderr, "ERROR: %s\n", e.what()); return 1; }
    fprintf(stderr, "Server: %s\n\n", qrpc::mac_to_string(server).c_str());

    // ── Trigger: heartbeat (void, no args) ─────────────────────────────
    fprintf(stderr, "--- trigger: heartbeat (0x03) ---\n");
    client.trigger(server, 0x03, {}, qrpc::ModeCode::FAST_TRIGGER);
    fprintf(stderr, "OK\n\n");

    // ── Trigger: add (uint32 + uint32 → uint32) ───────────────────────
    fprintf(stderr, "--- trigger: add (0x01, 17+25) ---\n");
    {
        auto args = qrpc::serialize_args(uint32_t(17), uint32_t(25));
        auto res = client.trigger(server, 0x01, args, qrpc::ModeCode::FAST_TRIGGER);
        if (!res.empty())
            fprintf(stderr, "Result: %u\n\n", qrpc::deserialize_result<uint32_t>(res));
        else
            fprintf(stderr, "No result\n\n");
    }

    // ── Data exchange: echo ────────────────────────────────────────────
    fprintf(stderr, "--- data exchange: echo (0x00) ---\n");
    {
        std::string msg = "Hello from qRPC!";
        auto args = qrpc::serialize_args(std::vector<uint8_t>(msg.begin(), msg.end()));
        auto res = client.data_exchange(server, 0x00, args, qrpc::ModeCode::FAST_DATA_EXCHANGE);
        if (!res.empty()) {
            auto blob = qrpc::deserialize_result<std::vector<uint8_t>>(res);
            fprintf(stderr, "Echo: \"%.*s\"\n\n", (int)blob.size(), blob.data());
        }
    }

    // ── Data exchange: reverse (slow mode) ─────────────────────────────
    fprintf(stderr, "--- data exchange: reverse (0x02, slow) ---\n");
    {
        auto args = qrpc::serialize_args(std::string("ABCDEF"));
        auto res = client.data_exchange(server, 0x02, args, qrpc::ModeCode::SLOW_DATA_EXCHANGE);
        if (!res.empty())
            fprintf(stderr, "Reversed: \"%s\"\n\n",
                    qrpc::deserialize_result<std::string>(res).c_str());
    }

    // ── Data exchange: multiply (double × double → double) ─────────────
    fprintf(stderr, "--- data exchange: multiply (0x04, 3.14*2.0) ---\n");
    {
        auto args = qrpc::serialize_args(3.14, 2.0);
        auto res = client.data_exchange(server, 0x04, args, qrpc::ModeCode::FAST_DATA_EXCHANGE);
        if (!res.empty())
            fprintf(stderr, "Result: %f\n\n", qrpc::deserialize_result<double>(res));
    }

    fprintf(stderr, "Client done.\n");
    return 0;
}
