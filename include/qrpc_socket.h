#pragma once

#include "qrpc_protocol.h"
#include <string>

namespace qrpc {

class RawSocket {
public:
    explicit RawSocket(const std::string& iface);
    ~RawSocket();
    RawSocket(const RawSocket&) = delete;
    RawSocket& operator=(const RawSocket&) = delete;

    ssize_t send_frame(const std::vector<uint8_t>& frame) const;
    RxFrame recv_frame(uint64_t timeout_us = 0) const;
    MacAddr local_mac() const { return local_mac_; }
    int fd() const { return fd_; }

private:
    int         fd_        = -1;
    int         ifindex_   = 0;
    MacAddr     local_mac_ = {};
    std::string iface_;
};

} // namespace qrpc
