
# qRPC: Quick Remote Procedure Call over link-layer 

## Introduction
Suppose we may have a local network or a computer cluster, and a necessity for *really fast* RPCs within it. For instance, a nuclear reactor where even a 0.1μs delay can cause a new Chernobyl.

There is plenty of techniques for achieving a fast RPC, including specialized hardware, optimized RTOSs and network stacks, userspace networking, and more. Yet there still is one issue: there is a computational and network overhead for TCP and even UDP connections.

qRPC is a custom level 2.5 network protocol that skips this overhead. qRPC stands for quick Remote Procedure Call.

qRPC communicates directly via the link layer, skipping entirely the network and transport layer. This both reduces the CPU cycles necessary for network- and transport-layer protocols, and the overhead of their headers in network traffic. The maximum frame size used is 1492 (-2*MAC_LENGTH - FCS_LENGTH), so it can be used with even many legacy network protocols.



## qRPC

qRPC is a Server-client protocol that supports two operations: trigger, and data-exchange. Each does what the name describes.

A Trigger, runs a procedure with a single network frame, and the client expects no response other than an ACK from the server. Ideal for functions that have no arguments -or arguments small enough to fit in a single network frame.

A Data-exchange, runs a procedure where at least one of the participators is likely to need more than one frame of data. That is, server procedures that need data in order to run, or return data that are necessary for the client, or both.

Both operations come, in turn, in a fast-mode version, where the requests and responses have no checksum other than the networks FCS, and a slow-mode version, where a checksum of two bytes is included. The operations and their mode are determined from a header.

The qRPC server signals its' presence by periodically broadcasting at a predefined time interval. Any client connected to the network after the server, waits until a message is received. The server can, theoretically, expose up to 65535 procedures and serve up to 65535 sessions simultaneously -where a session is a client invoking a particular procedure with a set of arguments.

Each session is uniquely identified by the MAC of the client and a client-specific session_id. This enables a client to invoke several procedures at once, or the same procedure with a different set of arguments.



## Potential real-life use cases of qRPC


#### 1. Nuclear reactors
If anything goes wrong in a nuclear reactor, a delay of even several microseconds can lead to a nuclear accident. Remotely triggering an operation that shuts off the operation as soon as possible is crucial to prevent nuclear accidents. Omitting the computational overhead that comes with conventional TCP or even UDP can speed up the operation.

#### 2. Missile intereception or other defence systems
In the era of drones and hypersonic missiles, intereception systems cannot possible waste milliseconds following conventional transport-layer protocols. When one or more sensors detect a threat, instantaneously reacting to it is a matter of life and death.

#### 3. Sensitive power grid operations
When a fault or anything unpredictable occurs on a high-voltage grid, immediately informing a central system of it or shutting off parts of the grid in advance to avoid cascades is a must. A delay can be catastrophic.



## Technical description of qRPC

See qRPC.md



## Sample implementation

This repository contains a sample implementation that was initially generated with Claude Opus 4.6, with several manual modifications and bugfixes added in advance. The prompt was the content of qRPC.md.


