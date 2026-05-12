Reverse Proxy Load Balancer in C

Overview

This project is an event-driven reverse proxy and load balancer written in C using Linux epoll.

The proxy accepts HTTP client connections, forwards requests to backend servers, receives backend responses, and sends them back to clients using non-blocking asynchronous I/O.

The project was built incrementally to learn low-level systems programming, Linux networking, socket programming, HTTP handling, and event-driven architecture.


Features

Networking

- TCP socket programming
- Non-blocking sockets
- Event-driven architecture using epoll
- Asynchronous backend connections
- HTTP request forwarding
- HTTP response forwarding

Load Balancing

- Round-robin load balancing
- Least-connections load balancing
- Backend selection metrics

Reliability

- Backend retry logic
- Backend health checking
- Timeout handling
- Connection cleanup
- Retry failover

HTTP Features

- HTTP header detection
- Request buffering
- Keep-alive client support
- Persistent client connections

Observability

- Structured logging
- Connection IDs
- Metrics/statistics
- Backend failure tracking
- Active connection tracking


Architecture

                +-------------------+
                |      Client       |
                +-------------------+
                          |
                          v
              +----------------------+
              |   Reverse Proxy      |
              |      (epoll)         |
              +----------------------+
                 |       |       |
        ----------       |       ----------
       |                  |                |
       v                  v                v

+-------------+   +-------------+   +-------------+
| Backend 1   |   | Backend 2   |   | Backend 3   |
|   :8081     |   |   :8082     |   |   :8083     |
+-------------+   +-------------+   +-------------+


Core Concepts Implemented

Event-Driven I/O

The project uses Linux epoll for scalable asynchronous socket handling.

Supported events:

- EPOLLIN
- EPOLLOUT


Non-Blocking Connections

All sockets are configured using:

fcntl(fd, F_SETFL, O_NONBLOCK);

Backend connections use asynchronous connect() with EINPROGRESS handling.


Request Flow

Client Request
    ↓
Proxy Receives Data
    ↓
HTTP Header Detection
    ↓
Backend Selection
    ↓
Request Forwarded
    ↓
Backend Response
    ↓
Response Sent To Client


Load Balancing Algorithms

Round Robin

Backends are selected sequentially.

Least Connections

Backends with the fewest active connections are preferred using a min-heap scheduler.


Backend Health Checking

Failed backends are temporarily marked unhealthy.

The proxy skips unhealthy backends for a cooldown period before retrying them.


Keep-Alive Support

The proxy supports persistent client connections.

A single client connection can send multiple HTTP requests without reconnecting.


Metrics Collected

The proxy tracks:

- Total connections
- Active connections
- Requests served
- Backend failures
- Connection timeouts
- Bytes transferred


Build Instructions

gcc -o proxy server.c


Running The Proxy

./proxy


Starting Backend Servers

Example Python backend servers:

python3 -m http.server 8081 &
python3 -m http.server 8082 &
python3 -m http.server 8083 &
python3 -m http.server 8084 &


Testing

Curl Test

curl http://127.0.0.1:9999/

Keep-Alive Test

curl -v --http1.1 http://127.0.0.1:9999/ http://127.0.0.1:9999/


Future Improvements

Possible future enhancements:

- TLS/HTTPS support
- Worker thread pools
- Configuration file support
- Rate limiting
- Zero-copy optimizations
- HTTP parsing improvements
- Backend connection pooling
- Statistics endpoint
- Health probe threads


Learning Goals

This project was built to learn:

- Linux systems programming
- Socket programming
- Event-driven networking
- Reverse proxy architecture
- Load balancing
- HTTP protocol basics
- Scalable server design
- Debugging low-level networking issues


Technologies Used

- C
- Linux
- epoll
- POSIX sockets
- TCP/IP
- HTTP/1.1
