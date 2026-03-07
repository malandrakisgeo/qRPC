#pragma once

#include "qrpc_protocol.h"
#include "qrpc_socket.h"

#include <atomic>
#include <map>
#include <vector>

namespace qrpc {

class Client {
public:
    explicit Client(const std::string& iface);
    ~Client() = default;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    MacAddr discover_server(uint64_t timeout_us = 5'000'000);

    std::vector<uint8_t> trigger(
        const MacAddr& server, uint16_t procedure_id,
        const std::vector<uint8_t>& args = {},
        ModeCode mode = ModeCode::FAST_TRIGGER,
        uint64_t timeout_us = 1'000'000);

    std::vector<uint8_t> data_exchange(
        const MacAddr& server, uint16_t procedure_id,
        const std::vector<uint8_t>& args,
        ModeCode mode = ModeCode::FAST_DATA_EXCHANGE,
        uint64_t timeout_us = 5'000'000);

private:
    uint16_t next_session_id();

    RxFrame wait_for(uint16_t sid, const std::vector<OpCode>& accept,
                     uint64_t timeout_us);

    std::vector<uint8_t> receive_multi_frame(
        const MacAddr& server, uint16_t sid, bool slow,
        uint16_t n_frames, uint64_t timeout_us);

    RawSocket sock_;
    std::atomic<uint16_t> session_counter_{1};
};

} // namespace qrpc
