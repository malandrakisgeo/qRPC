#pragma once

#include "qrpc_protocol.h"
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>

namespace qrpc {

// ═══════════════════════════════════════════════════════════════════════════
// Buffer reader/writer — sequential, network-order
// ═══════════════════════════════════════════════════════════════════════════

class BufferWriter {
public:
    void write_u8(uint8_t v)   { buf_.push_back(v); }
    void write_u16(uint16_t v) { v = htons(v); append(&v, 2); }
    void write_u32(uint32_t v) { v = htonl(v); append(&v, 4); }
    void write_u64(uint64_t v) {
        uint32_t hi = htonl(static_cast<uint32_t>(v >> 32));
        uint32_t lo = htonl(static_cast<uint32_t>(v));
        append(&hi, 4); append(&lo, 4);
    }
    void write_i8(int8_t v)    { write_u8(static_cast<uint8_t>(v)); }
    void write_i16(int16_t v)  { write_u16(static_cast<uint16_t>(v)); }
    void write_i32(int32_t v)  { write_u32(static_cast<uint32_t>(v)); }
    void write_i64(int64_t v)  { write_u64(static_cast<uint64_t>(v)); }
    void write_f32(float v)    { uint32_t u; memcpy(&u, &v, 4); write_u32(u); }
    void write_f64(double v)   { uint64_t u; memcpy(&u, &v, 8); write_u64(u); }
    void write_string(const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        buf_.insert(buf_.end(), s.begin(), s.end());
    }
    void write_blob(const std::vector<uint8_t>& v) {
        write_u32(static_cast<uint32_t>(v.size()));
        buf_.insert(buf_.end(), v.begin(), v.end());
    }
    const std::vector<uint8_t>& data() const { return buf_; }
    std::vector<uint8_t> take() { return std::move(buf_); }
private:
    void append(const void* p, size_t n) {
        auto b = static_cast<const uint8_t*>(p);
        buf_.insert(buf_.end(), b, b + n);
    }
    std::vector<uint8_t> buf_;
};

class BufferReader {
public:
    BufferReader(const uint8_t* d, size_t l) : data_(d), len_(l) {}
    explicit BufferReader(const std::vector<uint8_t>& v) : data_(v.data()), len_(v.size()) {}
    size_t remaining() const { return len_ - pos_; }

    uint8_t  read_u8()  { check(1); return data_[pos_++]; }
    uint16_t read_u16() { check(2); uint16_t v; memcpy(&v, data_+pos_, 2); pos_+=2; return ntohs(v); }
    uint32_t read_u32() { check(4); uint32_t v; memcpy(&v, data_+pos_, 4); pos_+=4; return ntohl(v); }
    uint64_t read_u64() { uint32_t hi = read_u32(), lo = read_u32(); return (uint64_t(hi)<<32)|lo; }
    int8_t   read_i8()  { return static_cast<int8_t>(read_u8()); }
    int16_t  read_i16() { return static_cast<int16_t>(read_u16()); }
    int32_t  read_i32() { return static_cast<int32_t>(read_u32()); }
    int64_t  read_i64() { return static_cast<int64_t>(read_u64()); }
    float    read_f32() { uint32_t u = read_u32(); float v; memcpy(&v, &u, 4); return v; }
    double   read_f64() { uint64_t u = read_u64(); double v; memcpy(&v, &u, 8); return v; }
    std::string read_string() { auto b = read_blob(); return {b.begin(), b.end()}; }
    std::vector<uint8_t> read_blob() {
        uint32_t n = read_u32(); check(n);
        std::vector<uint8_t> v(data_+pos_, data_+pos_+n); pos_+=n; return v;
    }
private:
    void check(size_t n) const {
        if (pos_+n > len_) throw std::runtime_error("BufferReader: past end");
    }
    const uint8_t* data_; size_t len_; size_t pos_ = 0;
};

// ═══════════════════════════════════════════════════════════════════════════
// Serialization traits
// ═══════════════════════════════════════════════════════════════════════════

template<typename T, typename = void> struct Marshal;

#define QRPC_MARSHAL_PRIM(TYPE, W, R) \
    template<> struct Marshal<TYPE> { \
        static void write(BufferWriter& w, TYPE v) { w.W(v); } \
        static TYPE read(BufferReader& r) { return r.R(); } \
    }

QRPC_MARSHAL_PRIM(uint8_t,  write_u8,  read_u8);
QRPC_MARSHAL_PRIM(uint16_t, write_u16, read_u16);
QRPC_MARSHAL_PRIM(uint32_t, write_u32, read_u32);
QRPC_MARSHAL_PRIM(uint64_t, write_u64, read_u64);
QRPC_MARSHAL_PRIM(int8_t,   write_i8,  read_i8);
QRPC_MARSHAL_PRIM(int16_t,  write_i16, read_i16);
QRPC_MARSHAL_PRIM(int32_t,  write_i32, read_i32);
QRPC_MARSHAL_PRIM(int64_t,  write_i64, read_i64);
QRPC_MARSHAL_PRIM(float,    write_f32, read_f32);
QRPC_MARSHAL_PRIM(double,   write_f64, read_f64);
#undef QRPC_MARSHAL_PRIM

template<> struct Marshal<bool> {
    static void write(BufferWriter& w, bool v) { w.write_u8(v ? 1 : 0); }
    static bool read(BufferReader& r) { return r.read_u8() != 0; }
};
template<> struct Marshal<std::string> {
    static void write(BufferWriter& w, const std::string& v) { w.write_string(v); }
    static std::string read(BufferReader& r) { return r.read_string(); }
};
template<> struct Marshal<std::vector<uint8_t>> {
    static void write(BufferWriter& w, const std::vector<uint8_t>& v) { w.write_blob(v); }
    static std::vector<uint8_t> read(BufferReader& r) { return r.read_blob(); }
};

// ═══════════════════════════════════════════════════════════════════════════
// wrap_procedure — typed callable → ProcedureFunc (bytes → ProcResult)
// ═══════════════════════════════════════════════════════════════════════════

namespace detail {

template<typename... Args>
std::tuple<std::decay_t<Args>...> deserialize_args(BufferReader& r) {
    return std::tuple<std::decay_t<Args>...>{ Marshal<std::decay_t<Args>>::read(r)... };
}

template<typename T>
std::vector<uint8_t> serialize_result(const T& val) {
    BufferWriter w; Marshal<std::decay_t<T>>::write(w, val); return w.take();
}

template<typename T> struct callable_traits;
template<typename C, typename R, typename... A>
struct callable_traits<R(C::*)(A...)>       { using ret = R; using args = std::tuple<A...>; };
template<typename C, typename R, typename... A>
struct callable_traits<R(C::*)(A...) const> { using ret = R; using args = std::tuple<A...>; };

template<typename Callable, typename R, typename... A>
ProcedureFunc wrap_impl(Callable fn, R*, std::tuple<A...>*) {
    auto sfn = std::function<R(A...)>(std::move(fn));
    return [sfn](const std::vector<uint8_t>& raw) -> ProcResult {
        try {
            BufferReader r(raw);
            auto args = deserialize_args<A...>(r);
            if constexpr (std::is_void_v<R>) {
                std::apply(sfn, std::move(args));
                return { {}, true };
            } else {
                R result = std::apply(sfn, std::move(args));
                return { serialize_result(result), true };
            }
        } catch (...) { return { {}, false }; }
    };
}

} // namespace detail

/// Wrap a function pointer.
template<typename R, typename... A>
ProcedureFunc wrap_procedure(R(*fn)(A...)) {
    return detail::wrap_impl(fn, static_cast<R*>(nullptr), static_cast<std::tuple<A...>*>(nullptr));
}

/// Wrap any callable (lambda, functor).
template<typename Callable,
         typename Tr = detail::callable_traits<decltype(&std::decay_t<Callable>::operator())>,
         typename R = typename Tr::ret, typename Args = typename Tr::args>
ProcedureFunc wrap_procedure(Callable&& fn) {
    return detail::wrap_impl(std::forward<Callable>(fn),
                             static_cast<R*>(nullptr), static_cast<Args*>(nullptr));
}

// ═══════════════════════════════════════════════════════════════════════════
// Client-side helpers
// ═══════════════════════════════════════════════════════════════════════════

template<typename... Args>
std::vector<uint8_t> serialize_args(const Args&... args) {
    BufferWriter w; (Marshal<std::decay_t<Args>>::write(w, args), ...); return w.take();
}

template<typename T>
T deserialize_result(const std::vector<uint8_t>& data) {
    BufferReader r(data); return Marshal<std::decay_t<T>>::read(r);
}

} // namespace qrpc
