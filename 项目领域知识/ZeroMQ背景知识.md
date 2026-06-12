# ZeroMQ (libzmq) 背景知识

## 什么是 ZeroMQ？

ZeroMQ（又称 ØMQ、ZMQ、0MQ）是一个**高性能异步消息传递库**，提供类似 Socket 的 API，但功能远超传统 Socket。它不是消息队列服务器，而是一个**嵌入式消息库**，可直接链接到应用程序中。

### 核心特性

| 特性 | 说明 |
|------|------|
| **多模式** | 支持 PUB/SUB、REQ/REP、PUSH/PULL、DEALER/ROUTER 等 20+ 种消息模式 |
| **多传输** | TCP、IPC、inproc（进程内）、WebSocket、UDP 等 |
| **多语言** | C/C++/Python/Java/Go/Rust/JavaScript 等 40+ 种语言绑定 |
| **异步 I/O** | 底层使用 epoll/kqueue/IOCP 等高效 I/O 多路复用 |
| **消息过滤** | 支持基于前缀的订阅过滤（PUB/SUB 模式） |
| **零拷贝** | 消息在进程间传递时避免不必要的内存复制 |

### 历史与版本

- **2007年**：由 iMatix 公司开发，作为 Open Messaging System 的一部分
- **2009年**：libzmq 2.0 发布，引入 ZMTP 协议
- **2013年**：libzmq 4.0 发布，支持 CURVE 安全机制
- **2016年**：libzmq 4.2 发布，性能优化和稳定性改进
- **2020年**：libzmq 4.3 发布，支持 ZMTP 3.1 协议
- **2024年**：libzmq 4.3.5，最新稳定版本

## ZMTP 协议

ZMTP（ZeroMQ Message Transport Protocol）是 ZeroMQ 的线协议，定义了消息的传输格式。

### ZMTP 3.1 版本特点

```
连接建立流程：
  Client                          Server
    |                               |
    |--- Greeting (64 bytes) ------>|
    |<-- Greeting (64 bytes) -------|
    |--- READY command ------------->|
    |<-- READY command -------------|
    |--- Message / Command -------->|
    |<-- Message / Command ---------|
    |           ...                  |
```

### Greeting 格式（64 字节）

```
Byte 0:    0xFF (signature)
Byte 1-8:  0x00 × 8 (reserved)
Byte 9:    0x7F (final short)
Byte 10:   revision (0x03 for ZMTP 3.0)
Byte 11:   0x00 for NULL / 0x01 for PLAIN / 0x02 for CURVE
Byte 12-31: 0x00 (reserved)
Byte 32-51: Server-Identity (20 bytes)
Byte 52-63: 0x00 (reserved)
```

## 架构设计

### 分层架构

```
┌─────────────────────────────────────────┐
│              Application                │
├─────────────────────────────────────────┤
│         Socket Types (19种)             │
│  PUB/SUB REQ/REP PUSH/PULL DEALER/ROUTER│
├─────────────────────────────────────────┤
│           Session Layer                 │
│    连接管理、会话状态、协议握手           │
├─────────────────────────────────────────┤
│           Engine Layer                  │
│    ZMTP 编解码、帧格式处理              │
├─────────────────────────────────────────┤
│           Transport Layer               │
│    TCP / IPC / inproc / WebSocket       │
├─────────────────────────────────────────┤
│           I/O Layer                     │
│    epoll / kqueue / IOCP / select       │
└─────────────────────────────────────────┘
```

### 线程模型

```
主线程 (Application Thread)
  │
  ├── I/O Thread 1 (epoll/kqueue)
  │   ├── Poller
  │   ├── Mailbox (命令队列)
  │   └── Session 1, 2, 3...
  │
  ├── I/O Thread 2
  │   ├── Poller
  │   ├── Mailbox
  │   └── Session 4, 5, 6...
  │
  └── Reaper Thread (对象回收)
```

### 核心组件

| 组件 | 文件 | 行数 | 职责 |
|------|------|------|------|
| `object_t` | `object.hpp` | ~200 | 命令消息收发基类 |
| `own_t` | `own.hpp` | ~150 | 对象生命周期树管理 |
| `socket_base_t` | `socket_base.cpp` | 2,218 | 所有 socket 类型的基类 |
| `session_base_t` | `session_base.cpp` | 796 | 连接会话管理 |
| `stream_engine_base_t` | `stream_engine_base.cpp` | 772 | 流引擎基类 |
| `ctx_t` | `ctx.cpp` | 886 | 全局上下文管理 |
| `pipe_t` | `pipe.cpp` | 605 | 内部管道连接 |

## 消息模式

### 1. PUB/SUB（发布/订阅）

```
Publisher                    Subscriber
   │                              │
   ├── publish("topic1 data") ──→ │ (过滤: topic1)
   ├── publish("topic2 data") ──→ │ (过滤: topic2)
   └── publish("topic1 data") ──→ │ (接收: topic1)
```

### 2. REQ/REP（请求/响应）

```
Requester                  Responder
   │                              │
   ├── send("request") ─────────→ │
   │                    ←─────── send("reply")
   ├── send("request") ─────────→ │
   │                    ←─────── send("reply")
```

### 3. PUSH/PULL（管道）

```
Ventilator              Worker 1    Worker 2    Worker 3
   │                      │            │            │
   ├── push(task1) ─────→ │            │            │
   ├── push(task2) ────── ────────→   │            │
   ├── push(task3) ────── ──────────────────→      │
   └── push(task4) ─────→ │            │            │
```

### 4. DEALER/ROUTER（路由）

```
Client 1    Client 2    Router              Service 1    Service 2
   │            │        │                    │            │
   └── send ────┼───────→│──── load balance ─→│            │
                └── send ─┼──── load balance ─┼───────→   │
                         │←───── reply ────────┘          │
                         │←───── reply ───────────────────┘
```

## 安全机制

### 1. NULL（无安全）

- 默认机制，无认证
- 适用于可信环境（如本地测试）

### 2. PLAIN（明文认证）

- 用户名/密码认证
- 适用于内部网络

### 3. CURVE（椭圆曲线加密）

- 基于 libsodium 的加密通信
- 支持前向保密（Perfect Forward Secrecy）
- 适用于不可信网络

### 4. ZAP（ZeroMQ Authentication Protocol）

- 集中式认证协议
- 支持自定义认证后端

## 性能特点

### 基准数据

```
=== inproc_lat (30B, 10k roundtrip) ===
average latency: 8.414 [us]

=== inproc_thr (30B, 100k msg) ===
mean throughput: 11,940,298 [msg/s]
mean throughput: 2865.672 [Mb/s]

=== benchmark_radix_tree (10k keys, 1M queries) ===
[trie]        Average lookup = 198.4 ns
[radix_tree]  Average lookup = 103.0 ns
```

### 优化技术

1. **VSM（Very Small Message）优化**：≤30 字节消息内联存储，零堆分配
2. **无锁数据结构**：yqueue、ypipe 等 SPSC 无锁队列
3. **批量处理**：减少系统调用次数
4. **零拷贝**：消息在进程间传递时避免复制
5. **缓存行对齐**：数据结构布局优化，减少缓存失效

## 与其他消息库对比

| 特性 | ZeroMQ | RabbitMQ | Kafka | Nanomsg |
|------|--------|----------|-------|---------|
| **架构** | 嵌入式库 | 独立服务器 | 独立服务器 | 嵌入式库 |
| **延迟** | 极低 (μs) | 中等 (ms) | 中等 (ms) | 极低 (μs) |
| **吞吐** | 极高 | 中等 | 极高 | 高 |
| **消息持久化** | 无 | 有 | 有 | 无 |
| **消息模式** | 丰富 | 有限 | 有限 | 丰富 |
| **协议** | ZMTP | AMQP | Kafka Protocol | SP |

## Rust 生态中的 ZeroMQ

### 现有实现

| 项目 | 特点 | 状态 |
|------|------|------|
| **zmq.rs** | 官方纯 Rust 实现，Sans-I/O 核心 | Beta |
| **rzmq** | 最活跃，io_uring 加速 | Beta |
| **omq.rs** | 架构最先进，声称 3x 性能 | Alpha |
| **rust-zmq** | C 绑定包装 | 稳定 |

### 设计模式对比

| 模式 | zmq.rs | rzmq | omq.rs | 本项目 |
|------|--------|------|--------|--------|
| **Sans-I/O** | 计划中 | 无 | 已实现 | 已实现 |
| **异步运行时** | Tokio/async-std | Tokio | Tokio/compio | Tokio |
| **io_uring** | 无 | 有 | 有 | 无 |
| **Socket 类型** | 11 | 8 | 20 | 19 |
| **安全机制** | NULL | NULL/PLAIN/CURVE/Noise | PLAIN/CURVE/BLAKE3 | NULL/PLAIN/CURVE |

## 为什么用 Rust 重写 libzmq？

### 优势

1. **内存安全**：Rust 的所有权系统在编译时防止数据竞争和内存泄漏
2. **零成本抽象**：泛型和 trait 提供零运行时开销的抽象
3. **并发安全**：`Send`/`Sync` trait 保证线程安全
4. **现代工具链**：Cargo 包管理、Clippy 代码检查、rustfmt 格式化
5. **跨平台**：统一的构建系统，支持 Linux/macOS/Windows

### 挑战

1. **unsafe 代码**：无锁数据结构和 FFI 需要 unsafe
2. **性能对齐**：需要达到 C++ 的性能水平
3. **协议兼容**：保持与 libzmq 的线级兼容
4. **生态成熟度**：Rust 异步生态相对较新

## 参考资源

- [ZeroMQ 官方文档](https://zeromq.org/documentation/)
- [ZMTP 协议规范](https://rfc.zeromq.org/spec:32/ZMTP/)
- [libzmq GitHub](https://github.com/zeromq/libzmq)
- [zmq.rs GitHub](https://github.com/zeromq/zmq.rs)
- [rzmq GitHub](https://github.com/excsn/rzmq)
- [omq.rs GitHub](https://github.com/Paddor/omq.rs)
