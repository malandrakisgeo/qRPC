#include "qrpc_socket.h"
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <poll.h>
#include <unistd.h>

namespace qrpc {

RawSocket::RawSocket(const std::string& iface) : iface_(iface) {
    fd_ = socket(AF_PACKET, SOCK_RAW, htons(QRPC_ETHERTYPE));
    if (fd_ < 0)
        throw std::runtime_error("socket(AF_PACKET): " + std::string(strerror(errno)));

    struct ifreq ifr{};
    strncpy(ifr.ifr_name, iface.c_str(), IFNAMSIZ - 1);

    if (ioctl(fd_, SIOCGIFINDEX, &ifr) < 0) {
        close(fd_); throw std::runtime_error("ioctl SIOCGIFINDEX: " + std::string(strerror(errno)));
    }
    ifindex_ = ifr.ifr_ifindex;

    if (ioctl(fd_, SIOCGIFHWADDR, &ifr) < 0) {
        close(fd_); throw std::runtime_error("ioctl SIOCGIFHWADDR: " + std::string(strerror(errno)));
    }
    memcpy(local_mac_.data(), ifr.ifr_hwaddr.sa_data, MAC_LEN);

    struct sockaddr_ll sll{};
    sll.sll_family   = AF_PACKET;
    sll.sll_ifindex  = ifindex_;
    sll.sll_protocol = htons(QRPC_ETHERTYPE);
    if (bind(fd_, reinterpret_cast<struct sockaddr*>(&sll), sizeof(sll)) < 0) {
        close(fd_); throw std::runtime_error("bind: " + std::string(strerror(errno)));
    }
}

RawSocket::~RawSocket() { if (fd_ >= 0) close(fd_); }

ssize_t RawSocket::send_frame(const std::vector<uint8_t>& frame) const {
    struct sockaddr_ll sll{};
    sll.sll_family  = AF_PACKET;
    sll.sll_ifindex = ifindex_;
    sll.sll_halen   = MAC_LEN;
    if (frame.size() >= MAC_LEN)
        memcpy(sll.sll_addr, frame.data(), MAC_LEN);
    return sendto(fd_, frame.data(), frame.size(), 0,
                  reinterpret_cast<struct sockaddr*>(&sll), sizeof(sll));
}

RxFrame RawSocket::recv_frame(uint64_t timeout_us) const {
    if (timeout_us > 0) {
        struct pollfd pfd{}; pfd.fd = fd_; pfd.events = POLLIN;
        int ms = static_cast<int>((timeout_us + 999) / 1000);
        int ret = poll(&pfd, 1, ms);
        if (ret == 0) throw std::runtime_error("recv timeout");
        if (ret < 0) throw std::runtime_error("poll: " + std::string(strerror(errno)));
    }
    uint8_t buf[2048];
    ssize_t n = recv(fd_, buf, sizeof(buf), 0);
    if (n < 0) throw std::runtime_error("recv: " + std::string(strerror(errno)));
    return parse_frame(buf, static_cast<size_t>(n));
}

} // namespace qrpc
