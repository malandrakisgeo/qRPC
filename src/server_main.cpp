#include "qrpc_server.h"
#include "qrpc_marshal.h"
#include <cstdio>
#include <csignal>
#include <string>

static qrpc::Server* g_server = nullptr;
static void on_signal(int) { if (g_server) g_server->stop(); }

// ── Plain typed functions — server never sees these types ──────────────────

static std::vector<uint8_t> echo(std::vector<uint8_t> data) {
    fprintf(stderr, "[proc 0x00] echo: %zu bytes\n", data.size());
    return data;
}

static uint32_t add(uint32_t a, uint32_t b) {
    fprintf(stderr, "[proc 0x01] add: %u + %u = %u\n", a, b, a + b);
    return a + b;
}

static std::string reverse_str(std::string s) {
    fprintf(stderr, "[proc 0x02] reverse: \"%s\"\n", s.c_str());
    return {s.rbegin(), s.rend()};
}

static void heartbeat() {
    fprintf(stderr, "[proc 0x03] heartbeat\n");
}

static double multiply(double a, double b) {
    fprintf(stderr, "[proc 0x04] multiply: %f * %f = %f\n", a, b, a * b);
    return a * b;
}

// ── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    const char* iface = (argc > 1) ? argv[1] : "eth0";
    fprintf(stderr, "=== qRPC server ===\nInterface: %s\n\n", iface);

    qrpc::Server server(iface, 1000);
    g_server = &server;
    signal(SIGINT, on_signal); signal(SIGTERM, on_signal);

    // Register via wrap_procedure — the server only stores ProcedureFunc.
    // Can also be called at runtime: server.register_procedure(0x10, wrap_procedure(fn));
    server.register_procedure(0x00, qrpc::wrap_procedure(echo));
    server.register_procedure(0x01, qrpc::wrap_procedure(add));
    server.register_procedure(0x02, qrpc::wrap_procedure(reverse_str));
    server.register_procedure(0x03, qrpc::wrap_procedure(heartbeat));
    server.register_procedure(0x04, qrpc::wrap_procedure(multiply));

    fprintf(stderr, "Procedures: 0x00=echo, 0x01=add, 0x02=reverse, "
                    "0x03=heartbeat, 0x04=multiply\n\n");

    server.start();
    fprintf(stderr, "Press Ctrl+C to stop.\n");
    pause();
    server.stop();
    return 0;
}
