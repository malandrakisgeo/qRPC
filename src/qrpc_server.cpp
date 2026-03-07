#include "qrpc_server.h"
#include <algorithm>
#include <cstdio>
#include <cstring>

namespace qrpc {

// ── Lifecycle ──────────────────────────────────────────────────────────────

Server::Server(const std::string& iface, uint32_t broadcast_interval_ms)
    : sock_(iface), broadcast_interval_ms_(broadcast_interval_ms) {}

Server::~Server() { stop(); }

void Server::register_procedure(uint16_t id, ProcedureFunc func) {
    std::lock_guard<std::mutex> lk(proc_mtx_);
    procedures_[id] = std::move(func);
}

void Server::unregister_procedure(uint16_t id) {
    std::lock_guard<std::mutex> lk(proc_mtx_);
    procedures_.erase(id);
}

ProcedureFunc Server::find_procedure(uint16_t id) {
    std::lock_guard<std::mutex> lk(proc_mtx_);
    auto it = procedures_.find(id);
    return it != procedures_.end() ? it->second : nullptr;
}

void Server::start() {
    running_ = true;
    rx_thread_ = std::thread(&Server::rx_loop, this);
    bc_thread_ = std::thread(&Server::broadcast_loop, this);
    hk_thread_ = std::thread(&Server::housekeeping_loop, this);
    fprintf(stderr, "[qrpc-server] started (MAC %s)\n",
            mac_to_string(sock_.local_mac()).c_str());
}

void Server::stop() {
    running_ = false;
    if (rx_thread_.joinable()) rx_thread_.join();
    if (bc_thread_.joinable()) bc_thread_.join();
    if (hk_thread_.joinable()) hk_thread_.join();
}

// ── Broadcast ──────────────────────────────────────────────────────────────

void Server::broadcast_loop() {
    auto frame = build_broadcast(sock_.local_mac());
    while (running_) {
        sock_.send_frame(frame);
        std::this_thread::sleep_for(std::chrono::milliseconds(broadcast_interval_ms_));
    }
}

// ── Housekeeping ───────────────────────────────────────────────────────────
// 1. RECEIVING_DATA idle ≥3ms → check missing frames / assemble
// 2. AWAITING_COMPLETION → exponential backoff re-send (0.1s..10s)
// 3. 30s lifetime → reap
void Server::housekeeping_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::microseconds(HOUSEKEEPING_INTERVAL_US));
        auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lk(session_mtx_);
        std::vector<SessionKey> reap;

        for (auto& [key, sess] : sessions_) {
            auto age = std::chrono::duration_cast<std::chrono::microseconds>(
                now - sess.created_at).count();
            if (static_cast<uint64_t>(age) > SESSION_TIMEOUT_US) {
                reap.push_back(key); continue;
            }

            auto idle = std::chrono::duration_cast<std::chrono::microseconds>(
                now - sess.last_activity).count();
            auto since_check = std::chrono::duration_cast<std::chrono::microseconds>(
                now - sess.last_missing_check).count();

            if (sess.phase == SessionPhase::RECEIVING_DATA &&
                static_cast<uint64_t>(idle) >= MISSING_FRAME_CHECK_US &&
                static_cast<uint64_t>(since_check) >= MISSING_FRAME_CHECK_US)
            {
                if (sess.rx_frames.complete())
                    try_assemble_and_run(sess);
                else {
                    send_resend_request(sess);
                    sess.last_missing_check = now;
                }
            }

            if (sess.phase == SessionPhase::AWAITING_COMPLETION &&
                static_cast<uint64_t>(idle) >= sess.response_retry_us)
            {
                if (sess.response_retry_us >= RESPONSE_RETRY_MAX_US) {
                    send_data_response(sess);   // final attempt
                    reap.push_back(key);
                } else {
                    send_data_response(sess);
                    sess.response_retry_us = std::min(
                        sess.response_retry_us * 2, RESPONSE_RETRY_MAX_US);
                    sess.last_activity = now;
                }
            }
        }
        for (auto& k : reap) sessions_.erase(k);
    }
}

// ── RX loop ────────────────────────────────────────────────────────────────

void Server::rx_loop() {
    while (running_) {
        try {
            RxFrame f = sock_.recv_frame(100'000); //EDW: ti sto anathema timeout einai auto?
            if (f.ethertype != QRPC_ETHERTYPE && f.ethertype != QRPC_ETHERTYPE_ALT) continue;
            if (f.magic != QRPC_MAGIC) continue;
            if (f.src_mac == sock_.local_mac()) continue;
            handle_frame(f);
        } catch (const std::runtime_error&) { /* recv timeout */ }
    }
}

void Server::handle_frame(const RxFrame& f) {
    switch (f.opcode) {
        case OpCode::SESSION_INIT:
            if (is_trigger_mode(f.mode))
                handle_trigger_init(f);
            else
                handle_data_init(f);
            break;
        case OpCode::SEQUEL_REQ:            handle_sequel(f);               break;
        case OpCode::SESSION_COMPLETION:    handle_session_completion(f);   break;
        case OpCode::SESSION_CANCELLATION:  handle_session_cancellation(f); break;
        case OpCode::ACK:                   handle_ack(f);                  break;
        default: break;
    }
}

// ── Trigger ────────────────────────────────────────────────────────────────

void Server::handle_trigger_init(const RxFrame& f) {
    if (f.version != QRPC_VERSION) return;
    SessionKey key{f.src_mac, f.session_id};
    bool slow = is_slow_mode(f.mode);

    // Checksum validation for slow mode
    if (slow && f.has_checksum && !f.checksum_valid) {
        send_error(f.src_mac, f.session_id, slow, MsgCode::REJECT_INIT);
        return;
    }

    {
        std::lock_guard<std::mutex> lk(session_mtx_);
        if (sessions_.count(key)) return;   // note 6: silent ignore
    }

    auto proc = find_procedure(f.proc_id);
    if (!proc) {
        send_error(f.src_mac, f.session_id, slow, MsgCode::REJECT_INIT);
        return;
    }

    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lk(session_mtx_);
        ServerSession& s = sessions_[key];
        s.key = key; s.mode = f.mode; s.procedure_id = f.proc_id;
        s.phase = SessionPhase::AWAITING_COMPLETION;
        s.created_at = now; s.last_activity = now;
    }

    ProcResult res = proc(f.data);
    send_trigger_response(f.src_mac, f.session_id, slow, res);

    // Store result for re-send by housekeeping if needed
    {
        std::lock_guard<std::mutex> lk(session_mtx_);
        auto it = sessions_.find(key);
        if (it != sessions_.end()) {
            it->second.result = res;
            it->second.result_ready = true;
        }
    }
}

void Server::send_trigger_response(const MacAddr& dst, uint16_t sid,
                                    bool slow, const ProcResult& res) {
    // Uses sequel_frame struct with opcode TRIGGER_RESP
    auto frame = build_sequel_frame(dst, sock_.local_mac(),
                                     OpCode::TRIGGER_RESP, sid,
                                     0 /*remaining*/, res.data.data(), res.data.size(), slow);
    sock_.send_frame(frame);
}

// ── Data-exchange init ─────────────────────────────────────────────────────

void Server::handle_data_init(const RxFrame& f) {
    if (f.version != QRPC_VERSION) return;
    SessionKey key{f.src_mac, f.session_id};
    bool slow = is_slow_mode(f.mode);

    if (slow && f.has_checksum && !f.checksum_valid) {
        send_error(f.src_mac, f.session_id, slow, MsgCode::REJECT_INIT);
        return;
    }

    {
        std::lock_guard<std::mutex> lk(session_mtx_);
        if (sessions_.count(key)) {
            send_ack(f.src_mac, f.session_id, slow);
            return;
        }
    }

    auto proc = find_procedure(f.proc_id);
    if (!proc) {
        send_error(f.src_mac, f.session_id, slow, MsgCode::REJECT_INIT);
        return;
    }

    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(session_mtx_);

    if (sessions_.size() >= MAX_SESSIONS) {
        send_error(f.src_mac, f.session_id, slow, MsgCode::REJECT_INIT);
        return;
    }

    ServerSession& s = sessions_[key];
    s.key = key; s.mode = f.mode; s.procedure_id = f.proc_id;
    s.phase = SessionPhase::RECEIVING_DATA;
    s.created_at = now; s.last_activity = now; s.last_missing_check = now;

    // Pre-allocate frame storage on the heap (spec note 8)
    if (!s.rx_frames.allocate(f.remaining_frames)) {
        sessions_.erase(key);
        send_memory_overflow(f.src_mac, f.session_id, slow);
        return;
    }

    // The init frame's own data is at countdown position remaining_frames
    if (!f.data.empty()) {
        if (!s.rx_frames.store(f.remaining_frames, f.data.data(), f.data.size())) {
            sessions_.erase(key);
            send_memory_overflow(f.src_mac, f.session_id, slow); //EDW: arxika estelne memory overflow, akoma kai an apla eixame duplicate
            return;
        }
    }

    send_ack(f.src_mac, f.session_id, slow);

    if (s.rx_frames.complete())
        try_assemble_and_run(s);
}

// ── Sequel ─────────────────────────────────────────────────────────────────

void Server::handle_sequel(const RxFrame& f) {
    SessionKey key{f.src_mac, f.session_id};

    std::lock_guard<std::mutex> lk(session_mtx_);
    auto it = sessions_.find(key);
    if (it == sessions_.end()) {
        //send_error(f.src_mac, f.session_id, false, MsgCode::REJECT_SEQUEL_INVALID, 0); //EDW: no need to pollute the network
        return;
    }

    ServerSession& s = it->second;
    bool slow = is_slow_mode(s.mode);

    if (s.phase != SessionPhase::RECEIVING_DATA) {
        send_ack(f.src_mac, f.session_id, slow);
        return;
    }

    // Validate checksum if slow mode
   // if (slow && f.has_checksum && !validate_sequel_checksum(f)) {
    if (slow && (!f.has_checksum || !validate_sequel_checksum(f))) {
        send_error(f.src_mac, f.session_id, slow,
                   MsgCode::REJECT_SEQUEL_INVALID, f.remaining_frames);
        return;
    }

    // Duplicate → ACK silently (spec edge-subcase-XX)
    if (s.rx_frames.is_duplicate(f.remaining_frames)) {
        //send_ack(f.src_mac, f.session_id, slow); //TODO: EDW: CHANGE THE SPEC
        return;
    }

    if (!f.data.empty()) {
        if (!s.rx_frames.store(f.remaining_frames, f.data.data(), f.data.size())) {
            send_error(f.src_mac, f.session_id, slow,
                       MsgCode::REJECT_SEQUEL_INVALID, f.remaining_frames);
            return;
        }
    }

    s.last_activity = std::chrono::steady_clock::now();
    send_ack(f.src_mac, f.session_id, slow);

    if (s.rx_frames.complete())
        try_assemble_and_run(s);
}

// ── Assemble & run ─────────────────────────────────────────────────────────

void Server::try_assemble_and_run(ServerSession& s) {
    s.phase = SessionPhase::EXECUTING;
    auto assembled = s.rx_frames.assemble();
    if(!s.rx_frames.missing().empty()){
        return; //EDW: Paraleipsh apo to Claude: xwris auto riskarame na kalesoume function me elliph data
    }

    auto proc = find_procedure(s.procedure_id);
   // if (!proc) { s.phase = SessionPhase::DONE; return; } //EDW: This should be impossible. Upotithetai to elegxoume euthus eksarxhs

    s.result = proc(assembled); 
    s.result_ready = true;
    s.phase = SessionPhase::AWAITING_COMPLETION;
    s.last_activity = std::chrono::steady_clock::now(); //TODO: edw: why is this necessary?
    s.response_retry_us = RESPONSE_RETRY_INIT_US; 

    send_data_response(s);
}

// ── Resend request ─────────────────────────────────────────────────────────

void Server::send_resend_request(ServerSession& s) {
    auto missing = s.rx_frames.missing();
    if (missing.empty()) return;
    bool slow = is_slow_mode(s.mode);

    // Spec: chunk if too many to fit in one frame
    size_t max_entries = (MAX_FRAME_PAYLOAD - 10 - (slow ? 1 : 0)) / 2;
    size_t offset = 0;
    while (offset < missing.size()) {
        size_t chunk = std::min(max_entries, missing.size() - offset);
        std::vector<uint16_t> sub(missing.begin() + offset,
                                  missing.begin() + offset + chunk);
        auto frame = build_resend_frames(s.key.client_mac, sock_.local_mac(),
                                          s.key.session_id, sub, slow);
        sock_.send_frame(frame);
        offset += chunk;
    }
}

// ── Data response to client ────────────────────────────────────────────────

void Server::send_data_response(ServerSession& s) {
    const auto& res = s.result;
    auto dst = s.key.client_mac;
    uint16_t sid = s.key.session_id;
    bool slow = is_slow_mode(s.mode);
    size_t cap = sequel_data_capacity(slow);

    uint16_t n_frames = res.data.empty() ? 0 :
        static_cast<uint16_t>((res.data.size() + cap - 1) / cap);

    if (n_frames == 0) {
        send_ack(dst, sid, slow);
        return;
    }

    if (n_frames > 1) {
        // Send DATA_RESP_INIT (session_generic with num = total frames)
        auto init = build_session_generic(dst, sock_.local_mac(),
                                           OpCode::DATA_RESP_INIT, sid,
                                           n_frames, 0, slow);
        sock_.send_frame(init);
    }

    // Send DATA_RESPONSE frames (sequel_frame layout)
    size_t offset = 0;
    for (uint16_t i = 0; i < n_frames; ++i) {
        uint16_t remaining = n_frames - i;
        size_t chunk = std::min(cap, res.data.size() - offset);
        auto frame = build_sequel_frame(dst, sock_.local_mac(),
                                         OpCode::DATA_RESPONSE, sid,
                                         remaining, res.data.data() + offset, chunk, slow);
        sock_.send_frame(frame);
        offset += chunk;
    }
}

// ── Session completion / cancellation / ack ────────────────────────────────

void Server::handle_session_completion(const RxFrame& f) {
    SessionKey key{f.src_mac, f.session_id};
    std::lock_guard<std::mutex> lk(session_mtx_);
    sessions_.erase(key);
}

void Server::handle_session_cancellation(const RxFrame& f) {
    SessionKey key{f.src_mac, f.session_id};
    std::lock_guard<std::mutex> lk(session_mtx_);
    sessions_.erase(key);
}

void Server::handle_ack(const RxFrame& f) {
    SessionKey key{f.src_mac, f.session_id};
    std::lock_guard<std::mutex> lk(session_mtx_);
    auto it = sessions_.find(key);
    if (it != sessions_.end())
        it->second.last_activity = std::chrono::steady_clock::now();
}

// ── Utility sends ──────────────────────────────────────────────────────────

void Server::send_ack(const MacAddr& dst, uint16_t sid, bool slow) {
    auto frame = build_session_generic(dst, sock_.local_mac(),
                                        OpCode::ACK, sid, 0, 0, slow);
    sock_.send_frame(frame);
}

void Server::send_error(const MacAddr& dst, uint16_t sid, bool slow,
                         MsgCode code, uint16_t frame_num) {
    auto frame = build_session_generic(dst, sock_.local_mac(),
                                        OpCode::ERROR_INFO, sid,
                                        frame_num, static_cast<uint16_t>(code), slow);
    sock_.send_frame(frame);
}

void Server::send_memory_overflow(const MacAddr& dst, uint16_t sid, bool slow) {
    auto frame = build_session_generic(dst, sock_.local_mac(),
                                        OpCode::MEMORY_OVERFLOW, sid, 0, 0, slow);
    sock_.send_frame(frame);
}

} // namespace qrpc
