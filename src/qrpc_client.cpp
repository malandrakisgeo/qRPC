#include "qrpc_client.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>

namespace qrpc {

Client::Client(const std::string& iface) : sock_(iface) {
    fprintf(stderr, "[qrpc-client] bound to %s (MAC %s)\n",
            iface.c_str(), mac_to_string(sock_.local_mac()).c_str());
}

uint16_t Client::next_session_id() {
    uint16_t id = session_counter_.fetch_add(1);
    if (id == 0) id = session_counter_.fetch_add(1);
    return id;
}

// ── Discover ───────────────────────────────────────────────────────────────

MacAddr Client::discover_server(uint64_t timeout_us) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::microseconds(timeout_us);
    while (std::chrono::steady_clock::now() < deadline) {
        auto rem = std::chrono::duration_cast<std::chrono::microseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (rem <= 0) break;
        try {
            RxFrame f = sock_.recv_frame(rem);
            //EDW: Allaksame to arxiko build_broadcast(). Des an xreiazontai peraiterw allages edw
            if ((f.ethertype == QRPC_ETHERTYPE || f.ethertype == QRPC_ETHERTYPE_ALT) &&
                f.opcode == OpCode::PERIODIC_BROADCAST) {
                fprintf(stderr, "[qrpc-client] discovered server: %s\n",
                        mac_to_string(f.src_mac).c_str());
                return f.src_mac;
            }
        } catch (...) {}
    }
    throw std::runtime_error("Server discovery timed out");
}

// ── Trigger ────────────────────────────────────────────────────────────────

std::vector<uint8_t> Client::trigger(
        const MacAddr& server, uint16_t procedure_id,
        const std::vector<uint8_t>& args,
        ModeCode mode, uint64_t timeout_us)
{
    uint16_t sid = next_session_id();
    bool slow = is_slow_mode(mode);

    auto send_init = [&]() {
        auto frame = build_session_init(server, sock_.local_mac(), mode, procedure_id,
                                         sid, 0 /*remaining=0 for trigger*/,
                                         args.data(), args.size());
        sock_.send_frame(frame);
    };

    send_init();

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::microseconds(timeout_us);
    uint64_t retry_us = 10;  // spec: 10μs

    while (std::chrono::steady_clock::now() < deadline) {
        try {
            RxFrame resp = wait_for(sid,
                {OpCode::TRIGGER_RESP, OpCode::ERROR_INFO, OpCode::ACK}, retry_us);

            if (resp.opcode == OpCode::ERROR_INFO) {
                fprintf(stderr, "[qrpc-client] trigger rejected\n");
                return {};
            }

            // Send session completion
            auto comp = build_session_generic(server, sock_.local_mac(),
                                               OpCode::SESSION_COMPLETION, sid, 0, 0, slow);
            sock_.send_frame(comp);

            // TRIGGER_RESP uses sequel_frame layout: data is in resp.data
            return resp.data;

        } catch (...) {
            send_init();
            retry_us = std::min<uint64_t>(retry_us * 2, 100'000);
        }
    }
    fprintf(stderr, "[qrpc-client] trigger: timeout\n");
    return {};
}

// ── Data exchange ──────────────────────────────────────────────────────────

std::vector<uint8_t> Client::data_exchange(
        const MacAddr& server, uint16_t procedure_id,
        const std::vector<uint8_t>& args,
        ModeCode mode, uint64_t timeout_us)
{
    uint16_t sid = next_session_id();
    bool slow = is_slow_mode(mode);
    size_t init_cap   = init_data_capacity(slow);
    size_t sequel_cap = sequel_data_capacity(slow);

    // Compute frame count
    uint16_t total_frames;
    if (args.size() <= init_cap) {
        total_frames = 1;
    } else {
        size_t leftover = args.size() - init_cap;
        total_frames = 1 + static_cast<uint16_t>((leftover + sequel_cap - 1) / sequel_cap);
    }

    // Build a map of all frames: countdown# → {offset, length}
    struct FRef { size_t off; size_t len; };
    std::map<uint16_t, FRef> frame_map;

    size_t first_chunk = std::min(args.size(), init_cap);
    frame_map[total_frames] = {0, first_chunk};
    {
        size_t off = first_chunk;
        uint16_t fn = total_frames - 1;
        while (off < args.size() && fn >= 1) {
            size_t chunk = std::min(sequel_cap, args.size() - off);
            frame_map[fn] = {off, chunk};
            off += chunk;
            if (fn == 0) break;
            --fn;
        }
    }

    // Helper: send init frame
    auto send_init = [&]() {
        auto frame = build_session_init(server, sock_.local_mac(), mode, procedure_id,
                                         sid, total_frames,
                                         args.data(), first_chunk);
        sock_.send_frame(frame);
    };

    // Helper: send sequel frame
    auto send_sequel = [&](uint16_t fn) {
        auto it = frame_map.find(fn);
        if (it == frame_map.end()) return;
        auto frame = build_sequel_frame(server, sock_.local_mac(),
                                         OpCode::SEQUEL_REQ, sid,
                                         fn, args.data() + it->second.off,
                                         it->second.len, slow);
        sock_.send_frame(frame);
    };

    // Helper: resend specific frames requested by server
    auto handle_resend_request = [&](const RxFrame& f) {
        auto missing = parse_resend_list(f.data);
        for (uint16_t fn : missing) {
            if (fn == total_frames) send_init();
            else send_sequel(fn);
        }
    };

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::microseconds(timeout_us);

    // ── Step 1: send init, wait for ACK ────────────────────────────────
    send_init();
    uint64_t retry_us = 3;  // spec: 3μs
    bool acked = false;

    while (!acked && std::chrono::steady_clock::now() < deadline) {
        try {
            RxFrame resp = wait_for(sid,
                {OpCode::ACK, OpCode::ERROR_INFO, OpCode::MEMORY_OVERFLOW}, retry_us);
            if (resp.opcode != OpCode::ACK) {
                fprintf(stderr, "[qrpc-client] data_exchange init rejected\n");
                return {};
            }
            acked = true;
        } catch (...) {
            send_init();
            retry_us = std::min<uint64_t>(retry_us * 2, 100'000);
        }
    }
    if (!acked) {
        fprintf(stderr, "[qrpc-client] data_exchange: no ACK for init\n");
        return {};
    }

    // ── Step 2: send sequel frames ─────────────────────────────────────
    {
        uint16_t fn = total_frames - 1;
        size_t off = first_chunk;

        while (off < args.size() && fn >= 1) {
            send_sequel(fn);

            retry_us = 100;
            bool frame_acked = false;
            for (int att = 0; att < 15 && !frame_acked; ++att) {
                try {
                    RxFrame resp = wait_for(sid,
                        {OpCode::ACK, OpCode::ERROR_INFO, OpCode::RESEND_FRAMES}, retry_us);

                    if (resp.opcode == OpCode::ACK) {
                        frame_acked = true;
                    } else if (resp.opcode == OpCode::RESEND_FRAMES) {
                        handle_resend_request(resp);
                    } else {
                        send_sequel(fn);
                    }
                } catch (...) {
                    send_sequel(fn);
                    retry_us = std::min<uint64_t>(retry_us * 2, 100'000);
                }
            }

            off += frame_map[fn].len;
            if (fn == 0) break;
            --fn;
        }
    }

    // ── Step 2b: brief window for late RESEND_FRAMES ───────────────────
    {
        auto resend_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(5);
        while (std::chrono::steady_clock::now() < resend_end) {
            try {
                RxFrame resp = wait_for(sid,
                    {OpCode::RESEND_FRAMES, OpCode::DATA_RESP_INIT,
                     OpCode::DATA_RESPONSE, OpCode::ACK}, 2'000);
                if (resp.opcode == OpCode::RESEND_FRAMES)
                    handle_resend_request(resp);
                else
                    break;  // got a data response, proceed to step 3
            } catch (...) { break; }
        }
    }

    // ── Step 3: receive response ───────────────────────────────────────
    retry_us = 100'000;
    RxFrame resp_frame{};
    bool got_resp = false;

    while (!got_resp && std::chrono::steady_clock::now() < deadline) {
        try {
            resp_frame = wait_for(sid,
                {OpCode::DATA_RESP_INIT, OpCode::DATA_RESPONSE,
                 OpCode::ACK, OpCode::ERROR_INFO}, retry_us);
            got_resp = true;
        } catch (...) {
            retry_us = std::min<uint64_t>(retry_us * 2, 1'000'000);
        }
    }
    if (!got_resp) {
        fprintf(stderr, "[qrpc-client] data_exchange: no response\n");
        auto canc = build_session_generic(server, sock_.local_mac(),
                                           OpCode::SESSION_CANCELLATION, sid, 0, 0, slow);
        sock_.send_frame(canc);
        return {};
    }

    std::vector<uint8_t> result;

    if (resp_frame.opcode == OpCode::DATA_RESPONSE) {
        // Single-frame: data is directly in resp_frame.data
        result = resp_frame.data;

    } else if (resp_frame.opcode == OpCode::DATA_RESP_INIT) {
        // Multi-frame: num field = total frames
        uint16_t n_frames = resp_frame.num;

        // ACK the init
        auto ack = build_session_generic(server, sock_.local_mac(),
                                          OpCode::ACK, sid, 0, 0, slow);
        sock_.send_frame(ack);

        result = receive_multi_frame(server, sid, slow, n_frames, timeout_us);

    } else if (resp_frame.opcode == OpCode::ACK) {
        // Empty result
    }

    // Session completion
    auto comp = build_session_generic(server, sock_.local_mac(),
                                       OpCode::SESSION_COMPLETION, sid, 0, 0, slow);
    sock_.send_frame(comp);
    return result;
}

// ── Receive multi-frame response ───────────────────────────────────────────

std::vector<uint8_t> Client::receive_multi_frame(
        const MacAddr& server, uint16_t sid, bool slow,
        uint16_t n_frames, uint64_t timeout_us)
{
    std::vector<std::vector<uint8_t>> slots(n_frames + 1);
    std::vector<bool> received(n_frames + 1, false);
    uint16_t count = 0;

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::microseconds(timeout_us);
    auto last_frame_time = std::chrono::steady_clock::now();

    while (count < n_frames && std::chrono::steady_clock::now() < deadline) {
        try {
            RxFrame df = wait_for(sid, {OpCode::DATA_RESPONSE}, 2'000);

            uint16_t rem = df.remaining_frames;
            if (rem >= 1 && rem <= n_frames && !received[rem]) {
                // Validate checksum if slow
                if (slow && df.has_checksum && !validate_sequel_checksum(df))
                    continue;  // drop invalid

                slots[rem] = df.data;
                received[rem] = true;
                ++count;
                last_frame_time = std::chrono::steady_clock::now();
            }
        } catch (...) {
            // Timeout — check if we should request missing frames (spec: after 3ms idle)
            auto idle = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - last_frame_time).count();

            if (static_cast<uint64_t>(idle) >= MISSING_FRAME_CHECK_US && count < n_frames) {
                std::vector<uint16_t> missing;
                for (uint16_t k = 1; k <= n_frames; ++k)
                    if (!received[k]) missing.push_back(k);
                if (!missing.empty()) {
                    auto frame = build_resend_frames(server, sock_.local_mac(),
                                                      sid, missing, slow);
                    sock_.send_frame(frame);
                    last_frame_time = std::chrono::steady_clock::now();
                }
            }
        }
    }

    // Assemble: highest countdown# = first data
    std::vector<uint8_t> result;
    for (int k = n_frames; k >= 1; --k)
        if (received[k])
            result.insert(result.end(), slots[k].begin(), slots[k].end());
    return result;
}

// ── Wait for frame ─────────────────────────────────────────────────────────

RxFrame Client::wait_for(uint16_t sid, const std::vector<OpCode>& accept,
                          uint64_t timeout_us) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::microseconds(timeout_us);
    while (std::chrono::steady_clock::now() < deadline) {
        auto rem = std::chrono::duration_cast<std::chrono::microseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (rem <= 0) break;

        RxFrame f = sock_.recv_frame(rem);
        if (f.ethertype != QRPC_ETHERTYPE && f.ethertype != QRPC_ETHERTYPE_ALT) continue;
        if (f.session_id != sid) continue;
        for (auto op : accept)
            if (f.opcode == op) return f;
    }
    throw std::runtime_error("wait_for: timeout");
}

} // namespace qrpc
