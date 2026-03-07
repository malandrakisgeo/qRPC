#include "qrpc_protocol.h"
#include "qrpc_server.h"     // FrameStore
#include "qrpc_marshal.h"
#include <cassert>
#include <cstdio>
#include <cstring>

static int tests_run = 0, tests_passed = 0;
#define TEST(name) do { ++tests_run; fprintf(stderr, "  %-55s", name); } while(0)
#define PASS() do { ++tests_passed; fprintf(stderr, "PASS\n"); } while(0)

using namespace qrpc;

// ═══════════════════════════════════════════════════════════════════════════
// Wire format / struct layout tests
// ═══════════════════════════════════════════════════════════════════════════

void test_broadcast() {
    TEST("broadcast: 3x qRPC_signature");
    MacAddr src = {0xAA,0xBB,0xCC,0xDD,0xEE,0xFF};
    auto raw = build_broadcast(src);
    // 14 (eth) + 12 (3×4) = 26
    assert(raw.size() == 26);
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::BROADCAST);
    assert(f.opcode == OpCode::PERIODIC_BROADCAST);
    assert(f.dst_mac == BROADCAST_MAC);
    assert(f.src_mac == src);
    PASS();
}

void test_session_init_fast() {
    TEST("init_req (SESSION_INIT, fast): wire layout");
    MacAddr dst = {1,2,3,4,5,6}, src = {0xA,0xB,0xC,0xD,0xE,0xF};
    uint8_t data[] = {0xDE, 0xAD};
    auto raw = build_session_init(dst, src, ModeCode::FAST_TRIGGER,
                                   0x0042, 99, 5, data, 2);
    // ETH(14) + magic(2)+opcode(2)+init(4)+sid(2)+rem(2)+dlen(2)+data(2) = 14+16 = 30
    assert(raw.size() == 30);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::INIT_REQ);
    assert(f.opcode == OpCode::SESSION_INIT);
    assert(f.version == QRPC_VERSION);
    assert(f.mode == ModeCode::FAST_TRIGGER);
    assert(f.proc_id == 0x0042);
    assert(f.session_id == 99);
    assert(f.remaining_frames == 5);
    assert(f.data_length == 2);
    assert(f.data.size() == 2);
    assert(f.data[0] == 0xDE && f.data[1] == 0xAD);
    assert(!f.has_checksum);
    PASS();
}

void test_session_init_slow() {
    TEST("init_req (SESSION_INIT, slow): checksum appended");
    MacAddr dst = {}, src = {};
    uint8_t data[] = {0x12, 0x34, 0x56};
    auto raw = build_session_init(dst, src, ModeCode::SLOW_TRIGGER,
                                   1, 7, 3, data, 3);
    // ETH(14) + 14 + 3 + 1(checksum) = 32
    assert(raw.size() == 32);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.has_checksum);
    assert(f.checksum_valid);
    assert(f.data.size() == 3);
    uint8_t expected_cs = 0x12 ^ 0x34 ^ 0x56;
    assert(f.checksum == expected_cs);
    PASS();
}

void test_session_init_slow_corrupt() {
    TEST("init_req (SESSION_INIT, slow): corrupted checksum");
    MacAddr dst = {}, src = {};
    uint8_t data[] = {0xAA, 0xBB};
    auto raw = build_session_init(dst, src, ModeCode::SLOW_DATA_EXCHANGE,
                                   1, 1, 1, data, 2);
    // Corrupt the last byte (checksum)
    raw.back() ^= 0xFF;
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.has_checksum);
    assert(!f.checksum_valid);
    PASS();
}

void test_session_init_no_data() {
    TEST("init_req (SESSION_INIT, fast, no data)");
    MacAddr dst = {}, src = {};
    auto raw = build_session_init(dst, src, ModeCode::FAST_TRIGGER,
                                   0, 0, 0, nullptr, 0);
    assert(raw.size() == 14 + 14);
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.data.empty());
    assert(f.remaining_frames == 0);
    PASS();
}

void test_sequel_frame() {
    TEST("sequel_frame (SEQUEL_REQ): wire layout");
    MacAddr dst = {}, src = {};
    uint8_t data[] = {1,2,3,4,5};
    auto raw = build_sequel_frame(dst, src, OpCode::SEQUEL_REQ, 42, 7,
                                   data, 5, false);
    // ETH(14) + magic(2)+sid(2)+opcode(2)+rem(2)+dlen(2)+data(5) = 14+15 = 29
    assert(raw.size() == 29);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::SEQUEL_FRAME);
    assert(f.opcode == OpCode::SEQUEL_REQ);
    assert(f.session_id == 42);
    assert(f.remaining_frames == 7);
    assert(f.data.size() == 5);
    assert(f.data[0] == 1 && f.data[4] == 5);
    PASS();
}

void test_trigger_resp() {
    TEST("sequel_frame (TRIGGER_RESP): round-trip");
    MacAddr dst = {}, src = {};
    uint8_t data[] = {0xFF, 0x00};
    auto raw = build_sequel_frame(dst, src, OpCode::TRIGGER_RESP, 100, 0,
                                   data, 2, true);
    // ETH(14) + 10 + 2 + 1(checksum) = 27
    assert(raw.size() == 27);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.opcode == OpCode::TRIGGER_RESP);
    assert(f.session_id == 100);
    assert(f.data.size() == 2);
    assert(f.has_checksum);
    assert(f.checksum == (0xFF ^ 0x00));
    PASS();
}

void test_data_response() {
    TEST("sequel_frame (DATA_RESPONSE): round-trip");
    MacAddr dst = {}, src = {};
    uint8_t data[] = {0xCA, 0xFE};
    auto raw = build_sequel_frame(dst, src, OpCode::DATA_RESPONSE, 200, 3,
                                   data, 2, false);
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::SEQUEL_FRAME);
    assert(f.opcode == OpCode::DATA_RESPONSE);
    assert(f.remaining_frames == 3);
    assert(f.data.size() == 2);
    PASS();
}

void test_session_generic_ack() {
    TEST("session_generic (ACK): wire layout");
    MacAddr dst = {}, src = {};
    auto raw = build_session_generic(dst, src, OpCode::ACK, 55, 0, 0, false);
    // ETH(14) + magic(2)+sid(2)+op(2)+num(2)+msgcode(2) = 14+10 = 24
    assert(raw.size() == 24);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::SESSION_GENERIC);
    assert(f.opcode == OpCode::ACK);
    assert(f.session_id == 55);
    assert(f.num == 0);
    assert(f.message_code == 0);
    PASS();
}

void test_session_generic_error() {
    TEST("session_generic (ERROR_INFO): num + message_code");
    MacAddr dst = {}, src = {};
    auto raw = build_session_generic(dst, src, OpCode::ERROR_INFO, 10,
                                      42, static_cast<uint16_t>(MsgCode::REJECT_SEQUEL_INVALID),
                                      false);
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.opcode == OpCode::ERROR_INFO);
    assert(f.num == 42);
    assert(f.message_code == static_cast<uint16_t>(MsgCode::REJECT_SEQUEL_INVALID));
    PASS();
}

void test_session_generic_data_resp_init() {
    TEST("session_generic (DATA_RESP_INIT): num = total frames");
    MacAddr dst = {}, src = {};
    auto raw = build_session_generic(dst, src, OpCode::DATA_RESP_INIT, 77,
                                      15 /*total frames*/, 0, false);
    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.opcode == OpCode::DATA_RESP_INIT);
    assert(f.session_id == 77);
    assert(f.num == 15);
    PASS();
}

void test_resend_frames() {
    TEST("RESEND_FRAMES: uint16 missing list");
    MacAddr dst = {}, src = {};
    std::vector<uint16_t> missing = {3, 7, 12};
    auto raw = build_resend_frames(dst, src, 99, missing, false);
    // ETH(14) + magic(2)+op(2)+sid(2)+rem(2)+dlen(2)+data(6) = 14+16 = 30
    assert(raw.size() == 30);

    RxFrame f = parse_frame(raw.data(), raw.size());
    assert(f.layout == FrameLayout::INIT_REQ);
    assert(f.opcode == OpCode::RESEND_FRAMES);
    assert(f.session_id == 99);
    assert(f.data.size() == 6);
    auto parsed = parse_resend_list(f.data);
    assert(parsed.size() == 3);
    assert(parsed[0] == 3 && parsed[1] == 7 && parsed[2] == 12);
    PASS();
}

void test_checksum_1byte() {
    TEST("checksum: single-byte XOR (per struct)");
    uint8_t data[] = {0x12, 0x34, 0x56, 0x78, 0x9A};
    uint8_t cs = xor_checksum(data, 5);
    assert(cs == (0x12 ^ 0x34 ^ 0x56 ^ 0x78 ^ 0x9A));
    PASS();
}

void test_mode_helpers() {
    TEST("is_slow_mode / is_trigger_mode");
    assert(is_slow_mode(ModeCode::SLOW_TRIGGER));
    assert(is_slow_mode(ModeCode::SLOW_DATA_EXCHANGE));
    assert(!is_slow_mode(ModeCode::FAST_TRIGGER));
    assert(is_trigger_mode(ModeCode::FAST_TRIGGER));
    assert(!is_trigger_mode(ModeCode::FAST_DATA_EXCHANGE));
    PASS();
}

void test_session_key() {
    TEST("SessionKey equality + hash");
    SessionKey k1 = {{1,2,3,4,5,6}, 100}, k2 = k1, k3 = {{1,2,3,4,5,6}, 101};
    assert(k1 == k2); assert(!(k1 == k3));
    SessionKeyHash h;
    assert(h(k1) == h(k2)); assert(h(k1) != h(k3));
    PASS();
}

void test_data_capacity() {
    TEST("data capacity helpers");
    assert(init_data_capacity(false) == MAX_FRAME_PAYLOAD - 14);
    assert(init_data_capacity(true)  == MAX_FRAME_PAYLOAD - 14 - 1);
    assert(sequel_data_capacity(false) == MAX_FRAME_PAYLOAD - 10);
    assert(sequel_data_capacity(true)  == MAX_FRAME_PAYLOAD - 10 - 1);
    PASS();
}

void test_parse_too_short() {
    TEST("parse_frame: rejects short data");
    uint8_t tiny[10] = {};
    bool threw = false;
    try { parse_frame(tiny, sizeof(tiny)); } catch (...) { threw = true; }
    assert(threw);
    PASS();
}

// ═══════════════════════════════════════════════════════════════════════════
// FrameStore tests  (uint16 keys per struct)
// ═══════════════════════════════════════════════════════════════════════════

void test_fs_allocate() {
    TEST("FrameStore: allocate + basic store");
    FrameStore fs;
    assert(fs.allocate(5));
    assert(!fs.complete());
    assert(fs.missing().size() == 5);
    uint8_t d[] = {0xAA};
    assert(fs.store(5, d, 1));
    assert(fs.count_received == 1);
    PASS();
}

void test_fs_duplicate() {
    TEST("FrameStore: rejects duplicates");
    FrameStore fs; fs.allocate(3);
    uint8_t d[] = {1,2};
    assert(fs.store(2, d, 2));
    //assert(!fs.store(2, d, 2)); //EDW: perhaps uncomment?
    assert(fs.is_duplicate(2));
    PASS();
}

void test_fs_range() {
    TEST("FrameStore: rejects out-of-range");
    FrameStore fs; fs.allocate(3);
    uint8_t d[] = {1};
    assert(!fs.store(0, d, 1));
    assert(!fs.store(4, d, 1));
    PASS();
}

void test_fs_assemble() {
    TEST("FrameStore: assemble reverse order");
    FrameStore fs; fs.allocate(3);
    uint8_t d3[] = {0xAA}, d2[] = {0xBB}, d1[] = {0xCC};
    fs.store(3, d3, 1); fs.store(1, d1, 1); fs.store(2, d2, 1);
    assert(fs.complete());
    auto a = fs.assemble();
    assert(a.size() == 3);
    assert(a[0] == 0xAA && a[1] == 0xBB && a[2] == 0xCC);
    PASS();
}

void test_fs_missing() {
    TEST("FrameStore: missing list");
    FrameStore fs; fs.allocate(5);
    uint8_t d[] = {0};
    fs.store(5, d, 1); fs.store(3, d, 1); fs.store(1, d, 1);
    auto m = fs.missing();
    assert(m.size() == 2 && m[0] == 2 && m[1] == 4);
    PASS();
}

// ═══════════════════════════════════════════════════════════════════════════
// Marshal tests
// ═══════════════════════════════════════════════════════════════════════════

void test_m_primitives() {
    TEST("marshal: primitives round-trip");
    { BufferWriter w; Marshal<uint32_t>::write(w, 12345);
      BufferReader r(w.data()); assert(Marshal<uint32_t>::read(r) == 12345); }
    { BufferWriter w; Marshal<int16_t>::write(w, -42);
      BufferReader r(w.data()); assert(Marshal<int16_t>::read(r) == -42); }
    { BufferWriter w; Marshal<double>::write(w, 3.14);
      BufferReader r(w.data()); double v = Marshal<double>::read(r);
      assert(v > 3.139 && v < 3.141); }
    PASS();
}

void test_m_string() {
    TEST("marshal: string round-trip");
    BufferWriter w; Marshal<std::string>::write(w, "hello qRPC");
    BufferReader r(w.data()); assert(Marshal<std::string>::read(r) == "hello qRPC");
    PASS();
}

void test_m_blob() {
    TEST("marshal: blob round-trip");
    std::vector<uint8_t> orig = {0xDE, 0xAD, 0xBE, 0xEF};
    BufferWriter w; Marshal<std::vector<uint8_t>>::write(w, orig);
    BufferReader r(w.data()); assert(Marshal<std::vector<uint8_t>>::read(r) == orig);
    PASS();
}

void test_m_serialize_args() {
    TEST("serialize_args multi-type");
    auto bytes = serialize_args(uint32_t(100), uint32_t(200));
    BufferReader r(bytes);
    assert(Marshal<uint32_t>::read(r) == 100);
    assert(Marshal<uint32_t>::read(r) == 200);
    PASS();
}

void test_m_wrap_typed() {
    TEST("wrap_procedure: typed function");
    auto wrapped = wrap_procedure(+[](uint32_t a, uint32_t b) -> uint32_t { return a + b; });
    auto args = serialize_args(uint32_t(17), uint32_t(25));
    auto res = wrapped(args);
    assert(res.success);
    assert(deserialize_result<uint32_t>(res.data) == 42);
    PASS();
}

void test_m_wrap_void() {
    TEST("wrap_procedure: void return");
    bool called = false;
    auto wrapped = wrap_procedure([&called]() -> void { called = true; });
    auto res = wrapped({});
    assert(res.success && res.data.empty() && called);
    PASS();
}

void test_m_wrap_string() {
    TEST("wrap_procedure: string -> string");
    auto wrapped = wrap_procedure(+[](std::string s) -> std::string {
        return {s.rbegin(), s.rend()};
    });
    auto res = wrapped(serialize_args(std::string("ABCDE")));
    assert(res.success);
    assert(deserialize_result<std::string>(res.data) == "EDCBA");
    PASS();
}

void test_m_wrap_bad_args() {
    TEST("wrap_procedure: bad args → failure");
    auto wrapped = wrap_procedure(+[](uint32_t, uint32_t) -> uint32_t { return 0; });
    auto res = wrapped({0x00, 0x01});
    assert(!res.success);
    PASS();
}

// ═══════════════════════════════════════════════════════════════════════════

int main() {
    fprintf(stderr, "\n=== qRPC Unit Tests ===\n\n");

    fprintf(stderr, "── Wire format / structs ──\n");
    test_broadcast();
    test_session_init_fast();
    test_session_init_slow();
    test_session_init_slow_corrupt();
    test_session_init_no_data();
    test_sequel_frame();
    test_trigger_resp();
    test_data_response();
    test_session_generic_ack();
    test_session_generic_error();
    test_session_generic_data_resp_init();
    test_resend_frames();
    test_checksum_1byte();
    test_mode_helpers();
    test_session_key();
    test_data_capacity();
    test_parse_too_short();

    fprintf(stderr, "\n── FrameStore ──\n");
    test_fs_allocate();
    test_fs_duplicate();
    test_fs_range();
    test_fs_assemble();
    test_fs_missing();

    fprintf(stderr, "\n── Marshal ──\n");
    test_m_primitives();
    test_m_string();
    test_m_blob();
    test_m_serialize_args();
    test_m_wrap_typed();
    test_m_wrap_void();
    test_m_wrap_string();
    test_m_wrap_bad_args();

    fprintf(stderr, "\n%d/%d tests passed.\n\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
