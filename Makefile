CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -O2
INCLUDES = -Iinclude
LDFLAGS  = -lpthread

SRCDIR   = src
BUILDDIR = build

COMMON   = $(SRCDIR)/qrpc_socket.cpp
SERVER   = $(COMMON) $(SRCDIR)/qrpc_server.cpp $(SRCDIR)/server_main.cpp
CLIENT   = $(COMMON) $(SRCDIR)/qrpc_client.cpp $(SRCDIR)/client_main.cpp
TESTS    = $(SRCDIR)/test_protocol.cpp

.PHONY: all clean test

all: $(BUILDDIR)/qrpc_server $(BUILDDIR)/qrpc_client $(BUILDDIR)/qrpc_tests

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/qrpc_server: $(SERVER) | $(BUILDDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

$(BUILDDIR)/qrpc_client: $(CLIENT) | $(BUILDDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

$(BUILDDIR)/qrpc_tests: $(TESTS) | $(BUILDDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

test: $(BUILDDIR)/qrpc_tests
	./$(BUILDDIR)/qrpc_tests

clean:
	rm -rf $(BUILDDIR)
