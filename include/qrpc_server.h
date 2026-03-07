#pragma once

#include "qrpc_protocol.h"
#include "qrpc_socket.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <new>
#include <thread>
#include <unordered_map>
#include <vector>

namespace qrpc {

// ═══════════════════════════════════════════════════════════════════════════
// FrameStore — pre-allocated indexed storage for multi-frame reassembly
//   Keys are uint16 remaining_frames countdown numbers (per struct).
//   Frame N = first data, frame 1 = last data.
// ═══════════════════════════════════════════════════════════════════════════

struct FrameStore {
    uint16_t total = 0;
    uint16_t count_received = 0;
    std::vector<std::vector<uint8_t>> slots;   // 1-indexed: [1..total]
    std::vector<bool>                 present;

    /// Pre-allocate.  Returns false on bad_alloc → caller sends MEMORY_OVERFLOW.
    bool allocate(uint16_t n_frames) {
        try {
            total = n_frames;
            slots.resize(n_frames + 1);
            present.assign(n_frames + 1, false);
            count_received = 0;
            return true;
        } catch (const std::bad_alloc&) { return false; }
    }

    bool store(uint16_t frame_num, const uint8_t* data, size_t len) {
        if (frame_num == 0 || frame_num > total) return false;
        if (present[frame_num]) return true; //EDW: initially false
        try {
            slots[frame_num].assign(data, data + len);
            present[frame_num] = true;
            ++count_received;
            return true;
        } catch (const std::bad_alloc&) { return false; }
    }

    bool is_duplicate(uint16_t fn) const { return fn > 0 && fn <= total && present[fn]; }
    bool complete() const { 
      /* Det här är felaktigt. 
        Complete = nothing is missing.
        Eite to missing einai adeio, eite to present[] ta exei ola.
        TODO: Anpassa så att vi kollar om present[] har allt istället.
        PS: Der erste der mir uber diesen Kommentar eine email schickt, bekommt $10
      */
     return count_received >= total && total > 0; 
    }

    std::vector<uint16_t> missing() const {
        std::vector<uint16_t> m; 
        for (uint16_t k = 1; k <= total; ++k)
            if (!present[k]) m.push_back(k);
        return m;
    }

    /// Assemble: highest frame# first (it's the first chunk of data).
    std::vector<uint8_t> assemble() const { //EDW: Mhpws auto riskarei na kalesei function me elliph data?
        std::vector<uint8_t> out;
        for (int k = total; k >= 1; --k)
            if (present[k])
                out.insert(out.end(), slots[k].begin(), slots[k].end());
        return out;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Session phases
// ═══════════════════════════════════════════════════════════════════════════

enum class SessionPhase {
    RECEIVING_DATA,
    EXECUTING,
    AWAITING_COMPLETION,
    DONE,
};

struct ServerSession {
    SessionKey    key;
    ModeCode      mode;
    uint16_t      procedure_id     = 0;
    SessionPhase  phase            = SessionPhase::RECEIVING_DATA;

    FrameStore    rx_frames;

    ProcResult    result;
    bool          result_ready       = false;
    uint64_t      response_retry_us  = RESPONSE_RETRY_INIT_US;

    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_activity;
    std::chrono::steady_clock::time_point last_missing_check;
};

// ═══════════════════════════════════════════════════════════════════════════
// Server
// ═══════════════════════════════════════════════════════════════════════════

class Server {
public:
    explicit Server(const std::string& iface, uint32_t broadcast_interval_ms = 1000);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void register_procedure(uint16_t id, ProcedureFunc func);
    void unregister_procedure(uint16_t id);
    void start();
    void stop();

private:
    void rx_loop();
    void broadcast_loop();
    void housekeeping_loop();

    void handle_frame(const RxFrame& f);
    void handle_trigger_init(const RxFrame& f);
    void handle_data_init(const RxFrame& f);
    void handle_sequel(const RxFrame& f);
    void handle_session_completion(const RxFrame& f);
    void handle_session_cancellation(const RxFrame& f);
    void handle_ack(const RxFrame& f);

    void send_ack(const MacAddr& dst, uint16_t sid, bool slow);
    void send_error(const MacAddr& dst, uint16_t sid, bool slow,
                    MsgCode code, uint16_t frame_num = 0);
    void send_memory_overflow(const MacAddr& dst, uint16_t sid, bool slow);
    void send_trigger_response(const MacAddr& dst, uint16_t sid,
                               bool slow, const ProcResult& res);
    void send_data_response(ServerSession& sess);
    void send_resend_request(ServerSession& sess);
    void try_assemble_and_run(ServerSession& sess);

    ProcedureFunc find_procedure(uint16_t id);

    RawSocket sock_;
    uint32_t  broadcast_interval_ms_;

    std::mutex proc_mtx_;
    std::unordered_map<uint16_t, ProcedureFunc> procedures_;

    std::mutex session_mtx_;
    std::unordered_map<SessionKey, ServerSession, SessionKeyHash> sessions_;

    std::atomic<bool> running_{false};
    std::thread rx_thread_, bc_thread_, hk_thread_;
};

} // namespace qrpc
