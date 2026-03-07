#### Important note: this is more of a draft for the first version
There may be small discrepancies, and there are in all probability edge-cases that have not yet been handled.



## OPERATIONS, MODES, AND MODE CODES
Trigger-operation: client calls (i.e. triggers) procedure with few or no arguments and the procedure is void -or with its' returned data are irrelevant. Only an ACK (or error) is expected from the server .
Data-exchange operation: client calls a procedure in order to fetch data from it (i.e. retrieves data from a remote machine), with or without data as arguments

Fast-mode: No request/response checksum validation - Relying on networks FCS.
Slow-mode: Checksum validation - A naive 8bit XOR of the content in this early version. A 16bit XOR or Digest is a future plan.

Used with ethertypes: 0x88B5, 0x88B6

Qrpc-magic-number aka QPRC_SIGNATURE
0x8899

## BASIC HEADERS

0x00 slow-mode trigger
0x11 fast-mode trigger
0xEE fast-mode data-exchange
0xFF slow-mode data-exchange



## OPERATION CODES

CLIENT-SEND
session_init 0x0101
sequel_req  0x0202
session_completion 0x0404
session_cancellation 0x0606

SERVER-SEND
trigger_resp  0x0707
data_resp_init 0x0808
data_response 0x0811
periodic_broadcast 0x9999


COMMON
ack_header 0x0909
error_info 0x1111
resend_frames 0x2020
memory_overflow 0x3232



# MESSAGE CODES

0xF45F server rejecting a trigger/data-exchange initialization request.
0xF56F server rejecting a sequel request as invalid (frame number filled in the num field)
0xF34F server rejecting a cancellation or session termination
0xE45E Client rejecting a data packet from server as invalid (frame_number filled in the num field)



Headers:
2 bytes (unsigned short for session number)
2 bytes for mode-code
2 bytes for operation header
2 bytes for qRPC version


QPRC_SIGNATURE
qrpc-magic-number+qrpc-operation-header

VERSION_MODECODE (if init operation)
2 or 3 bytes







## TRIGGER MODE

Request:

QPRC_SIGNATURE (session_init_header)
OPERATION_CODE
qRPC_init (qprc version, mode_code  0x0101, procedure identifier)
Client Session number, chosen by the client, depending on whether it has othe opened sessions
Zero
Total length of rest of the data (procedure ID, arguments, and checksum if one exists)
 data (arguments)
?hash or XOR of the previous data for validation?




Response:
QPRC_SIGNATURE (trigger_resp)
 Session number (mporei o client na ekane duo triggers -prepei na kserei poio afora h apanthsh)
operation code 0x0808
zero
data length
data
checksum, if slow mode








## DATA SEND- OR EXCHANGE MODE:

Client requests:

SINGLE_OR_INITIAL_REQ :

0.QPRC_SIGNATURE (session_init_header)
operation_code 0x0101
qrpc_version, modecode, remote procedure ID,
session number
remaining_frames
data_length of current frame
data
hash or XOR of the previous data (slow mode only)


SEQUEL_FRAME
QPRC_SIGNATURE (sequel_req_header)
 Session number
 opcode 0x0202
 Countdown num of remaining frames
data_length of current frame
data
hash or XOR of the previous data (slow mode only)

The server always checks fields 0 and 1, and checks if there is an active session with the same source and session number. If there isn't one, the rest of the frame is treated a SINGLE_OR_INITIAL_REQ. Otherwise, the server checks whether there is the header of a session completion or cancellation, and if there isn't one, it is treated as a SEQUEL_REQUEST.


Responses:

ACK:
QPRC_SIGNATURE (ack_header)
 Session number
0x0909 (ack_header)
 XOR checksum of the previous data if slow mode





SESSION_COMPLETION
 QPRC_SIGNATURE for session completion
Session number
0x0404
 XOR checksum of the previous data if slow mode


SESSION_CANCELLATION
 QPRC_SIGNATURE
 Session number
 0x0606


rejection response for invalid frame:
QPRC_SIGNATURE
Session number
0x1111
number of problematic frame
0xF56F
checksum


BROADCAST
periodic_broadcast
 QPRC_SIGNATURE + QPRC_SIGNATURE + QPRC_SIGNATURE
once every seconkd




## Trigger mode:

Normal scenario
Client sends a trigger request to Server. Repeatedly every 10μs, until a response is received.
If anything with the request is wrong, a rejection is returned by the server and the session is terminated.
Examples of wrong requests:
    (0. Wrong version or qRPC)
    1. Invalid args or data - Validation failure

Else:
Server stores the client/session_number tuple and ignores other triggers
with the same client and session number until a session completion is received

Server immediately runs the procedure and returns a response (or just ACK).

Client it expected to ACK the response by completting the session. If no completion is received in reasonable time (TODO: how much time is that?), the response is sent again. The server waits for  twice the reasonable time, iterating again until the time exceeds 30s, and then a final response is sent before the session is terminated by the server.



### Example of a trigger mode
Client sends a data tranfer request to Server. Every 10μs, until a response is received
Server stores the client/session_number tuple and ignores other initialization triggers with the same client and session number
Server runs the procedure, and returns a response.
For simplicity reasons, the server does not expect an ACK response from the client -only a session completion/termination signal from it.
Only a session termination packet is expected -and is equivalent to an ACK.

Edge-subcase-1:
If no session termination is received within 5μs, the server resends the response. If no session termination is again received after 10μs, the server resends the response. The waiting time is doubled until it exceeds 15 secs, whereupon the session is terminated by the server.
The client has to keep track of the responses it received, by their frame number (an ascending int). For a particular session, frames with a given number are read only once.
If the client sends then a session termination request, it is rejected with a header indicative of invalid session. The client does not resend it then.

Edge-subcase-2:
If the client does not receive a response, it triggers the procedure again.





## Data exchange mode

1. Client sends an initialization request (session_init), every 3μs until a response is received.
2. Server stores the client/session_number tuple and ignores other session_init triggers with the same client and session number
3. Server uses the remaining_frames header of the init request to preallocate enough memory on the heap -for the data, and their frame number. A memory overflow error is returned if that is impossible.
Otherwise  an ACK is returned,  hence informing of the established session. The ACK is sent repeatedly, until the first sequel_request is received. It is sent in the same manner as edge-subcase-1: after 0.1s, then 0.2s, etc, up to 10s, until a SEQUEL_REQUEST is received.
4. Client the next requests frames (SEQUEL_REQUEST) sequintially. The server stores them in a map, with their number as a key. When the client in a particular session stops sending requests for 3ms, the server checks whatever has been received
    If there are missing frames
        A 0x2020 with the numbers of the missing frames is sent. If those missing are so many that a single frame does not fit their numbers, a single frame is sent with the first ones, and then subsequent 0x2020s follow.

    If no frames are missing,
       The server assembles the data in reverse (the first packet is mapped to the original remaining_frames number -the largest one). It then runs the procedure and stores the result.

5. The server returns the result to the client:

    If multiple frames are needed,
        A data_resp_init with the total number of frames is sent.
        The client allocates memory for a map and ACKs or rejects. Session termination on rejection.
        On ACK,the server sends them (operation 0x0811), and the client receives them in the same manner as the server did (map, check for missing frames, etc)


    If just one frame is needed,
        A single 0x0811 is send from the server, repeatedly. It is sent in the same manner as edge-subcase-1: after 0.1s, then 0.2s, etc, up to 10s, until a SESSION_TERMINATION is received.
        And the session is terminated by the server after the waiting time exceeds the 10s.



### Edge-subcase-XX (unit test)
A server rejects a sequel_request, but the packet does not reach the client in time. The server resends the rejection, and the client eventually receives the first message, whereupon it resends the rejected frame. The server accepts it. But then the client receives the initial rejection -which was stuck in a network node. The client will the resend the packet, along with the next ones for which no ACK has been received.

TLDR, the client will always resend the failed frames no matter how many times they failed. The server will keep track of the frames it successfully read before, via their ascending ID, and ignore them if the same ID is received again from a particular client/session_id tuple.




## Structs to use
The structs used for implementations *should* look like below:

struct qRPC_signature {
    uint16 qrpc_magic_number; 
    uint16 operation_code;
};

struct qRPC_init {
    uint8 qrpc_version;
    uint8 mode_code;
    uint16 proc_id;
};


struct init_req {
    uint16 qrpc_magic_number;
    uint16 operation_code;
    qRPC_init init; //If the operation_code is SESSION_INIT, this field exists. Otherwise it doesn't.
    uint16 session_id;
    uint16 remaining_frames; //Zero for triggers
    uint16 data_length;
    char[data_length] data; //If the operation is not 0x2020 (resend_frames)
    uint16[data_length / sizeof(uint16)] missing_frames; //If the operation is 0x2020 (resend_frames)
    char checksum; //non-zero if mode_code is SLOW_MODE, else it is data.
};

struct sequel_frame { //aka trigger_resp
    uint16 qrpc_magic_number;
    uint16 session_id;
    uint16 operation_code;
    uint16 remaining_frames;
    uint16 data_length;
    char[data_length] data;
    char checksum; //non-zero if mode_code is SLOW_MODE, else omitted.
};


struct session_generic {
    uint16 qrpc_magic_number;
    uint16 session_id;
    uint16 operation_code;
    uint16 num; //the number of the problematic frame for error_info operation (0x1111), otherwise the total number of frames to be sent for data_resp_init (0x0808)
    uint16 message_code; //non-zero if operation is error_info (0x1111) or memory_overflow 0x3232
    char checksum; //non-zero if mode_code is SLOW_MODE, else omitted.
};






------------------------------------------------------



## Notes (both explanations and technical details):

1. Data validation
Given the low Bit Error Rate in modern hardware and the FCS in modern networks, data validation is optional. It would most likely introduce unnecessary computational and memory overhead. Yet since there is a non-zero probability that a some transmission error does somehow give the same CRC code, it might be good to allow for a 'slow mode' that uses extra checksum validation, similar to most network protocols.

2. Session number
The same machine may want to invoke separate procedures at the same time, or may want to invoke one procedure with two different sets of arguments. That is why we need a session number.

3. Trigger mode vs data exchange mode
The trigger mode is meant for void procedures, or procedures returning just an int or up to several bytes no more than a single network frame, and at the same time the arguments are not data meant to be stored.

Data exchange mode is used whenever the procedure returns data that are of interest for the client, and whenever one wants to send data as arguments.


 4. Network/application security is taken for granted, by design.
 qPRC does not concern itself with solving the problem of malicious actors. Yes, a malicious actor in the network may spoof a MAC and trigger procedures as DDoS. He/she may also send malware in the data.
These problems shall be solved on the network- or application-level. qPRC assumes security issues in the network or application are solved.


5. Countdown number as unsigned char
qPRC is currently not suitable for exchanging large amounts of data.
If you are likely to send or receive more than 256 network frames (about 2kbs each in most protocols), a stable TCP connection is more reliable.
Future versions may use an unsigned short thereby allowing for up to 65535 frames.

6. Requests of a particular device for reinitializing the same session, will be silently ignored

7. Fast mode vs slow mode
The fast mode (no checksum for the data) is suitable in the following cases:
    1. Specialized network hardware where Bit Error Rate is zero or close to zero.
    2. Custom FCS protocols where it is mathematically impossible to not catch an error
    3. Triggers that will fail if there is any discrepancy at the bit level, e.g. called with the wrong int argument.


8. Frame ordering and missing frames
When the session for a data-exchange is established, the client has specified the number of the frames to be sent. The server expects that very number of frames, and allocates enough memory on the heap to store them along with their frame numbers in a Map. A memory_overflow error is returned if the allocation cannot be done.

Each frame that reaches the server has the number of remaining_frames in the header. That header, in essence, states the position of the frame content in the final data.

If a received frame is valid, the server stores it in the map, using the remaining_frames as a key. After all the frames have been received, or 2ms have passed without newer frames arriving, the server lists the missing frame numbers. If there are any, it sends a 0x2020 request where those frames are the missing data. The process is repeated, until all have arrived.

This solves even the frame ordering problem.

The server then assembles their data and sends them to the procedure. Keep in mind that the assemblement is done in reverse: the first frame has the largest number, the last frame the number 1.

The client does something similar when receiving the data:

9. Checksum
The checksum is a single XOR of all the data. If the operation is a fast mode operation, it is ignored.
For slow-mode operations, the received frame is re-XORed and the results are compared. A 0xF56F is returned

10. Maximum frame size:
Default should be at most 1492, headers included, so virtually all network protocols  canuse it. But perhaps the user should be able to give a custom size as argument

------------------------------------------------------
