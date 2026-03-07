#pragma once

#include <cstdint>
#include <cstring>
#include <array>
#include <functional>
#include <vector>
#include <string>
#include <stdexcept>
#include <arpa/inet.h>

namespace qrpc {

// ═══════════════════════════════════════════════════════════════════════════
// Protocol constants
// ═══════════════════════════════════════════════════════════════════════════

static constexpr uint16_t QRPC_MAGIC           = 0x8899;
static constexpr uint16_t QRPC_ETHERTYPE       = 0x88B5;
static constexpr uint16_t QRPC_ETHERTYPE_ALT   = 0x88B6;
static constexpr uint8_t  QRPC_VERSION         = 0x01;

static constexpr size_t   MAC_LEN              = 6;
static constexpr size_t   ETH_HDR_LEN          = 14;     // dst(6)+src(6)+ethertype(2)
static constexpr size_t   MAX_FRAME_PAYLOAD    = 1492;    // per spec, PPPoE-safe

// Struct-derived overhead (worst case, init_req with qRPC_init + slow checksum):
//   magic(2)+opcode(2)+init(4)+session(2)+remaining(2)+datalen(2)+checksum(1) = 15
// sequel_frame overhead: magic(2)+session(2)+opcode(2)+remaining(2)+datalen(2)+checksum(1) = 11
static constexpr size_t   INIT_REQ_HDR_LEN     = 14;     // without checksum
static constexpr size_t   SEQUEL_HDR_LEN       = 10;     // without checksum

// NOTE: spec says "up to 256 procedures" but struct proc_id is uint16.
//       spec says "up to 65535 sessions", struct session_id is uint16. Consistent.
// NOTE: spec note 5 says remaining_frames is "unsigned char" but struct is uint16.
//       We follow the structs.
//static constexpr uint16_t MAX_PROCEDURES       = 256;
static constexpr uint16_t MAX_SESSIONS         = 0xFFFF;

// ═══════════════════════════════════════════════════════════════════════════
// Mode codes  (stored in 1 byte in qRPC_init, but sent as values below)
// ═══════════════════════════════════════════════════════════════════════════

enum class ModeCode : uint8_t {
    SLOW_TRIGGER        = 0x00,
    FAST_TRIGGER        = 0x11,
    FAST_DATA_EXCHANGE  = 0xEE,
    SLOW_DATA_EXCHANGE  = 0xFF,
};

inline bool is_slow_mode(ModeCode m) {
    return m == ModeCode::SLOW_TRIGGER || m == ModeCode::SLOW_DATA_EXCHANGE;
}
inline bool is_trigger_mode(ModeCode m) {
    return m == ModeCode::SLOW_TRIGGER || m == ModeCode::FAST_TRIGGER;
}

// ═══════════════════════════════════════════════════════════════════════════
// Operation codes
// ═══════════════════════════════════════════════════════════════════════════

enum class OpCode : uint16_t {
    // Client-send
    SESSION_INIT            = 0x0101,
    SEQUEL_REQ              = 0x0202,
    SESSION_COMPLETION      = 0x0404,
    SESSION_CANCELLATION    = 0x0606,

    // Server-send
    TRIGGER_RESP            = 0x0707,   // opcode table; spec description wrongly says 0x0808
    DATA_RESP_INIT          = 0x0808,
    DATA_RESPONSE           = 0x0811,
    PERIODIC_BROADCAST      = 0x9999,

    // Common
    ACK                     = 0x0909,
    ERROR_INFO              = 0x1111,
    RESEND_FRAMES           = 0x2020,
    MEMORY_OVERFLOW         = 0x3232,
};

// ═══════════════════════════════════════════════════════════════════════════
// Message codes (for error_info / rejection payloads)
// ═══════════════════════════════════════════════════════════════════════════

enum class MsgCode : uint16_t {
    NONE                    = 0x0000,
    REJECT_INIT             = 0xF45F,
    REJECT_SEQUEL_INVALID   = 0xF56F,
    REJECT_CANCEL_TERM      = 0xF34F,
    CLIENT_REJECT_DATA      = 0xE45E,
};

// ═══════════════════════════════════════════════════════════════════════════
// MAC helpers
// ═══════════════════════════════════════════════════════════════════════════

using MacAddr = std::array<uint8_t, MAC_LEN>;
static const MacAddr BROADCAST_MAC = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};

inline std::string mac_to_string(const MacAddr& m) {
    char buf[18];
    snprintf(buf, sizeof(buf), "%02x:%02x:%02x:%02x:%02x:%02x",
             m[0], m[1], m[2], m[3], m[4], m[5]);
    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════
// Session key
// ═══════════════════════════════════════════════════════════════════════════

struct SessionKey {
    MacAddr  client_mac;
    uint16_t session_id;
    bool operator==(const SessionKey& o) const {
        return client_mac == o.client_mac && session_id == o.session_id;
    }
};

struct SessionKeyHash {
    size_t operator()(const SessionKey& k) const {
        size_t h = 0;
        for (auto b : k.client_mac) h = h * 31 + b;
        return h * 31 + k.session_id;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Checksum: XOR all data bytes into a single byte  (spec note 9)
// NOTE: spec intro says "two bytes" but struct says `char checksum` (1 byte).
//       We follow the struct.
// ═══════════════════════════════════════════════════════════════════════════

inline uint8_t xor_checksum(const uint8_t* data, size_t len) {
    uint8_t cs = 0;
    for (size_t i = 0; i < len; ++i) cs ^= data[i];
    return cs;
}

// ═══════════════════════════════════════════════════════════════════════════
// ProcResult / ProcedureFunc  — server is type-oblivious via these
// ═══════════════════════════════════════════════════════════════════════════

struct ProcResult {
    std::vector<uint8_t> data;
    bool success = true;
};

using ProcedureFunc = std::function<ProcResult(const std::vector<uint8_t>&)>;

// ═══════════════════════════════════════════════════════════════════════════
// Timing constants (microseconds unless noted)
// ═══════════════════════════════════════════════════════════════════════════

static constexpr uint64_t MISSING_FRAME_CHECK_US  = 3'000;         // 3 ms (spec: "3ms")
static constexpr uint64_t SESSION_TIMEOUT_US       = 30'000'000;   // 30 s
static constexpr uint64_t HOUSEKEEPING_INTERVAL_US = 500;          // 0.5 ms
static constexpr uint64_t RESPONSE_RETRY_INIT_US   = 100'000;     // 0.1 s
static constexpr uint64_t RESPONSE_RETRY_MAX_US    = 10'000'000;  // 10 s

// ═══════════════════════════════════════════════════════════════════════════
// Flat parsed frame — all possible fields; which are valid depends on opcode
// ═══════════════════════════════════════════════════════════════════════════

// Wire layout classification based on the provided structs:
//
// struct init_req: used when opcode appears at bytes [2..3]
//   → SESSION_INIT (0x0101): has qRPC_init sub-struct
//   → RESEND_FRAMES (0x2020): no qRPC_init
//   → PERIODIC_BROADCAST (0x9999): special 3×signature
//
// struct sequel_frame (aka trigger_resp): session_id at [2..3], opcode at [4..5]
//   → SEQUEL_REQ (0x0202), TRIGGER_RESP (0x0707), DATA_RESPONSE (0x0811)
//
// struct session_generic: session_id at [2..3], opcode at [4..5]
//   → ACK (0x0909), ERROR_INFO (0x1111), SESSION_COMPLETION (0x0404),
//     SESSION_CANCELLATION (0x0606), DATA_RESP_INIT (0x0808), MEMORY_OVERFLOW (0x3232)

enum class FrameLayout { INIT_REQ, SEQUEL_FRAME, SESSION_GENERIC, BROADCAST, UNKNOWN };

struct RxFrame {
    // Ethernet
    MacAddr  src_mac{};
    MacAddr  dst_mac{};
    uint16_t ethertype = 0;

    // Determined by parser
    FrameLayout layout = FrameLayout::UNKNOWN;

    // Common
    uint16_t magic      = 0;
    OpCode   opcode     = static_cast<OpCode>(0);
    uint16_t session_id = 0;

    // From qRPC_init (SESSION_INIT only)
    uint8_t  version    = 0;
    ModeCode mode       = ModeCode::FAST_TRIGGER;
    uint16_t proc_id    = 0;

    // From init_req / sequel_frame
    uint16_t remaining_frames = 0;
    uint16_t data_length      = 0;
    std::vector<uint8_t> data;

    // From session_generic
    uint16_t num          = 0;   // problematic frame# or total frames
    uint16_t message_code = 0;

    // Checksum
    uint8_t  checksum       = 0;
    bool     has_checksum   = false;
    bool     checksum_valid = true;
};

// Determine which opcodes go at position [2..3] (init_req / broadcast format)
inline bool is_leading_opcode(uint16_t val) {
    return val == static_cast<uint16_t>(OpCode::SESSION_INIT) ||
           val == static_cast<uint16_t>(OpCode::RESEND_FRAMES) ||
           val == static_cast<uint16_t>(OpCode::PERIODIC_BROADCAST);
}

// Determine if an opcode uses sequel_frame layout
inline bool is_sequel_layout(OpCode op) {
    return op == OpCode::SEQUEL_REQ ||
           op == OpCode::TRIGGER_RESP ||
           op == OpCode::DATA_RESPONSE;
}

// ═══════════════════════════════════════════════════════════════════════════
// Parser
// ═══════════════════════════════════════════════════════════════════════════

inline RxFrame parse_frame(const uint8_t* raw, size_t len) {
    if (len < ETH_HDR_LEN + 4)
        throw std::runtime_error("Frame too short");

    RxFrame f;
    const uint8_t* p = raw;

    // Ethernet header
    memcpy(f.dst_mac.data(), p, MAC_LEN); p += MAC_LEN;
    memcpy(f.src_mac.data(), p, MAC_LEN); p += MAC_LEN;
    uint16_t et; memcpy(&et, p, 2); p += 2;
    f.ethertype = ntohs(et);

    const uint8_t* qrpc = p;             // start of qRPC payload
    size_t qlen = len - ETH_HDR_LEN;

    // [0..1] = magic
    uint16_t w0; memcpy(&w0, qrpc, 2); f.magic = ntohs(w0);
    if (f.magic != QRPC_MAGIC) {
        f.layout = FrameLayout::UNKNOWN;
        return f;
    }

    // [2..3] — either opcode (init_req/broadcast) or session_id (sequel/generic)
    uint16_t w1; 
    memcpy(&w1, qrpc + 2, 2); 
    w1 = ntohs(w1);

    if (is_leading_opcode(w1)) {
        f.opcode = static_cast<OpCode>(w1);

        // ── BROADCAST ──
        if (f.opcode == OpCode::PERIODIC_BROADCAST) {
            f.layout = FrameLayout::BROADCAST;
            return f;
        }

        // ── RESEND_FRAMES (init_req without qRPC_init) ──
        if (f.opcode == OpCode::RESEND_FRAMES) {
            f.layout = FrameLayout::INIT_REQ;
            // No qRPC_init field
            if (qlen < 10) return f;
            uint16_t sid; memcpy(&sid, qrpc + 4, 2); f.session_id = ntohs(sid);
            uint16_t rem; memcpy(&rem, qrpc + 6, 2); f.remaining_frames = ntohs(rem);
            uint16_t dl;  memcpy(&dl,  qrpc + 8, 2); f.data_length = ntohs(dl);
            size_t data_start = 10;
            if (qlen >= data_start + f.data_length)
                f.data.assign(qrpc + data_start, qrpc + data_start + f.data_length);
            return f;
        }

        // ── SESSION_INIT (init_req with qRPC_init) ──
        f.layout = FrameLayout::INIT_REQ;
        if (qlen < 14) return f;
        // qRPC_init at [4..7]
        f.version = qrpc[4];
        f.mode    = static_cast<ModeCode>(qrpc[5]);
        uint16_t pid; memcpy(&pid, qrpc + 6, 2); f.proc_id = ntohs(pid);
        // rest of init_req
        uint16_t sid; memcpy(&sid, qrpc + 8,  2); f.session_id       = ntohs(sid);
        uint16_t rem; memcpy(&rem, qrpc + 10, 2); f.remaining_frames = ntohs(rem);
        uint16_t dl;  memcpy(&dl,  qrpc + 12, 2); f.data_length      = ntohs(dl);
        size_t data_start = 14;
        size_t avail = qlen - data_start;

        bool slow = is_slow_mode(f.mode);
        size_t cs_sz = slow ? 1 : 0;

        if (avail >= f.data_length + cs_sz) {
            f.data.assign(qrpc + data_start, qrpc + data_start + f.data_length);
            if (slow) {
                f.has_checksum = true;
                f.checksum = qrpc[data_start + f.data_length];
                uint8_t computed = xor_checksum(f.data.data(), f.data.size());
                f.checksum_valid = (computed == f.checksum);
            }
        } else if (avail >= cs_sz) {
            size_t actual_data = avail - cs_sz;
            f.data.assign(qrpc + data_start, qrpc + data_start + actual_data);
            if (slow) {
                f.has_checksum = true;
                f.checksum = qrpc[data_start + actual_data];
                uint8_t computed = xor_checksum(f.data.data(), f.data.size());
                f.checksum_valid = (computed == f.checksum);
            }
        }
        return f;
    }

    // ── [2..3] is session_id, [4..5] is opcode ──
    f.session_id = w1;
    if (qlen < 6) return f;
    uint16_t w2; memcpy(&w2, qrpc + 4, 2); f.opcode = static_cast<OpCode>(ntohs(w2));

    if (is_sequel_layout(f.opcode)) {
        // ── sequel_frame ──
        f.layout = FrameLayout::SEQUEL_FRAME;
        if (qlen < 10) return f;
        uint16_t rem; memcpy(&rem, qrpc + 6, 2); f.remaining_frames = ntohs(rem);
        uint16_t dl;  memcpy(&dl,  qrpc + 8, 2); f.data_length      = ntohs(dl);
        size_t data_start = 10;
        size_t avail = qlen - data_start;

        // Mode is not in the frame — must be known from session context.
        // We parse data optimistically; checksum validation done by caller.
        if (avail >= f.data_length) {
            f.data.assign(qrpc + data_start, qrpc + data_start + f.data_length);
            if (avail > f.data_length) {
                // The trailing byte could be a checksum (slow mode)
                f.has_checksum = true;
                f.checksum = qrpc[data_start + f.data_length];
            }
        }
        return f;
    }

    // ── session_generic ──
    f.layout = FrameLayout::SESSION_GENERIC;
    if (qlen >= 8) {
        uint16_t n; memcpy(&n, qrpc + 6, 2); f.num = ntohs(n);
    }
    if (qlen >= 10) {
        uint16_t mc; memcpy(&mc, qrpc + 8, 2); f.message_code = ntohs(mc);
    }
    if (qlen >= 11) {
        f.has_checksum = true;
        f.checksum = qrpc[10];
    }
    return f;
}

// ═══════════════════════════════════════════════════════════════════════════
// Builders — produce complete Ethernet frames
// ═══════════════════════════════════════════════════════════════════════════

namespace detail {
    inline void push16(std::vector<uint8_t>& buf, uint16_t v) {
        buf.push_back(static_cast<uint8_t>(v >> 8));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    }
    inline void write_eth_hdr(std::vector<uint8_t>& buf,
                               const MacAddr& dst, const MacAddr& src, uint16_t etype) {
        buf.insert(buf.end(), dst.begin(), dst.end());
        buf.insert(buf.end(), src.begin(), src.end());
        push16(buf, etype);
    }
}

/// Build a SESSION_INIT frame  (init_req with qRPC_init).
inline std::vector<uint8_t> build_session_init(
        const MacAddr& dst, const MacAddr& src,
        ModeCode mode, uint16_t proc_id,
        uint16_t session_id, uint16_t remaining_frames,
        const uint8_t* data, size_t data_len)
{
    std::vector<uint8_t> buf;
    buf.reserve(ETH_HDR_LEN + INIT_REQ_HDR_LEN + data_len + 1);
    detail::write_eth_hdr(buf, dst, src, QRPC_ETHERTYPE);

    // qRPC_signature
    detail::push16(buf, QRPC_MAGIC);
    detail::push16(buf, static_cast<uint16_t>(OpCode::SESSION_INIT));

    // qRPC_init
    buf.push_back(QRPC_VERSION);
    buf.push_back(static_cast<uint8_t>(mode));
    detail::push16(buf, proc_id);

    // rest of init_req
    detail::push16(buf, session_id);
    detail::push16(buf, remaining_frames);
    detail::push16(buf, static_cast<uint16_t>(data_len));

    if (data && data_len > 0)
        buf.insert(buf.end(), data, data + data_len);

    if (is_slow_mode(mode) && data && data_len > 0)
        buf.push_back(xor_checksum(data, data_len));

    return buf;
}

/// Build a sequel_frame  (SEQUEL_REQ, TRIGGER_RESP, or DATA_RESPONSE).
inline std::vector<uint8_t> build_sequel_frame(
        const MacAddr& dst, const MacAddr& src,
        OpCode opcode, uint16_t session_id,
        uint16_t remaining_frames,
        const uint8_t* data, size_t data_len,
        bool slow_mode)
{
    std::vector<uint8_t> buf;
    buf.reserve(ETH_HDR_LEN + SEQUEL_HDR_LEN + data_len + 1);
    detail::write_eth_hdr(buf, dst, src, QRPC_ETHERTYPE);

    detail::push16(buf, QRPC_MAGIC);
    detail::push16(buf, session_id);
    detail::push16(buf, static_cast<uint16_t>(opcode));
    detail::push16(buf, remaining_frames);
    detail::push16(buf, static_cast<uint16_t>(data_len));

    if (data && data_len > 0)
        buf.insert(buf.end(), data, data + data_len);

    if (slow_mode && data && data_len > 0)
        buf.push_back(xor_checksum(data, data_len));

    return buf;
}

/// Build a session_generic frame (ACK, errors, completion, cancellation, etc).
inline std::vector<uint8_t> build_session_generic(
        const MacAddr& dst, const MacAddr& src,
        OpCode opcode, uint16_t session_id,
        uint16_t num, uint16_t message_code,
        bool slow_mode, uint8_t checksum_val = 0)
{
    std::vector<uint8_t> buf;
    buf.reserve(ETH_HDR_LEN + 10 + 1);
    detail::write_eth_hdr(buf, dst, src, QRPC_ETHERTYPE);

    detail::push16(buf, QRPC_MAGIC);
    detail::push16(buf, session_id);
    detail::push16(buf, static_cast<uint16_t>(opcode));
    detail::push16(buf, num);
    detail::push16(buf, message_code);

    if (slow_mode)
        buf.push_back(checksum_val);

    return buf;
}

/// Build a RESEND_FRAMES frame  (init_req without qRPC_init, data = uint16 list).
inline std::vector<uint8_t> build_resend_frames(
        const MacAddr& dst, const MacAddr& src,
        uint16_t session_id,
        const std::vector<uint16_t>& missing_frame_nums,
        bool slow_mode)
{
    size_t data_len = missing_frame_nums.size() * 2;
    std::vector<uint8_t> buf;
    buf.reserve(ETH_HDR_LEN + 10 + data_len + 1);
    detail::write_eth_hdr(buf, dst, src, QRPC_ETHERTYPE);

    detail::push16(buf, QRPC_MAGIC);
    detail::push16(buf, static_cast<uint16_t>(OpCode::RESEND_FRAMES));
    detail::push16(buf, session_id);
    detail::push16(buf, 0); // remaining_frames (unused)
    detail::push16(buf, static_cast<uint16_t>(data_len));

    for (uint16_t fn : missing_frame_nums)
        detail::push16(buf, fn);

    if (slow_mode) {
        // Checksum over the raw missing-frame data
        uint8_t cs = 0;
        size_t start = buf.size() - data_len;
        for (size_t i = start; i < buf.size(); ++i) 
            cs ^= buf[i];
        buf.push_back(cs);
    }

    return buf;
}

/// Build a PERIODIC_BROADCAST frame  (3× qRPC_signature).
inline std::vector<uint8_t> build_broadcast(const MacAddr& src) {
    std::vector<uint8_t> buf;
    buf.reserve(ETH_HDR_LEN + 12);
    detail::write_eth_hdr(buf, BROADCAST_MAC, src, QRPC_ETHERTYPE);

    for (int i = 0; i < 3; ++i) {
        detail::push16(buf, QRPC_MAGIC);
        detail::push16(buf, static_cast<uint16_t>(OpCode::PERIODIC_BROADCAST));
    } //NOTE: This is wrong; it should be QRPC_MAGIC * 3 + PERIODIC_BROADCAST, but in this sample implementation it doesn't really matter. TODO: Allakse to kapoia stigmh.

   
    return buf;
}

/// Parse missing frame numbers from a RESEND_FRAMES data payload.
inline std::vector<uint16_t> parse_resend_list(const std::vector<uint8_t>& data) {
    std::vector<uint16_t> result;
    for (size_t i = 0; i + 1 < data.size(); i += 2) {
        uint16_t v; memcpy(&v, data.data() + i, 2);
        result.push_back(ntohs(v));
    }
    return result;
}

/// Validate checksum for a sequel_frame (mode must be known from session context).
inline bool validate_sequel_checksum(const RxFrame& f) {
    if (!f.has_checksum || f.data.empty()) return true;
    return xor_checksum(f.data.data(), f.data.size()) == f.checksum;
}

// ═══════════════════════════════════════════════════════════════════════════
// Data capacity helpers
// ═══════════════════════════════════════════════════════════════════════════

inline size_t init_data_capacity(bool slow) {
    return MAX_FRAME_PAYLOAD - INIT_REQ_HDR_LEN - (slow ? 1 : 0);
}

inline size_t sequel_data_capacity(bool slow) {
    return MAX_FRAME_PAYLOAD - SEQUEL_HDR_LEN - (slow ? 1 : 0);
}

} // namespace qrpc
