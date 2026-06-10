# 总体方案：libzmq → Rust 重写

## 一、项目背景与目标

### 1.1 源代码规模
| 模块 | 文件数 | 代码行数 |
|------|--------|----------|
| `src/*.cpp` | 122 | 36,299 |
| `src/*.hpp` | 155 | 14,143 |
| **核心库合计** | **277** | **~50,442** |
| `tests/*.cpp` | 142 | 23,136 |
| `unittests/*.cpp` | 7 | 2,491 |
| `perf/*.cpp` | 8 | 1,354 |
| **测试/基准合计** | **157** | **~27,000** |

### 1.2 核心架构模块（C++ 原版）
```
zmq::ctx_t          — 全局上下文（socket 容器、IO 线程池、reaper）
zmq::socket_base_t  — 所有 socket 类型的基类（connect/bind/send/recv）
zmq::msg_t          — 消息封装（零拷贝、引用计数、VSM 优化）
zmq::pipe_t         — 双向无锁管道（基于 yqueue/ypipe）
zmq::session_base_t — 连接会话管理（管理 peer 连接、创建 transport engine）
zmq::stream_engine_t— TCP/IPC 传输引擎（ZMTP 协议实现）
zmq::poller_t       — I/O 多路复用抽象（epoll/kqueue/poll/select 等 6 种后端）
zmq::mechanism_t    — 安全机制抽象（NULL / PLAIN / CURVE / GSSAPI）
zmq::lb_t / fq_t    — 负载均衡 / 公平队列消息调度器
zmq::object_t       — 基于命令消息的线程间通信基础设施
zmq::own_t          — 对象生命周期树（父子 shutdown 握手）
```

19 种 socket 类型：PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB, STREAM, SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, DGRAM, PEER, CHANNEL

### 1.3 三大核心参考项目

| 项目 | 状态 | 套接字 | 传输 | 安全 | 亮点 |
|------|------|--------|------|------|------|
| **zmq.rs** (zeromq 官方) v0.6.0 | Beta | 11 种 | TCP, IPC | NULL | GenericSocketBackend + FairQueue + WriteQueue 模式成熟 |
| **rzmq** v0.5.18 | Beta 活跃 | 8 种 | TCP, IPC, inproc | NULL, PLAIN, CURVE | io_uring 加速，最活跃维护 |
| **omq.rs** v0.5.3 | Alpha | 20 种(最全) | TCP, IPC, inproc, UDP, WS, WSS | PLAIN, CURVE | **Sans-I/O ZMTP 核心**（架构最先进） |

---

## 二、总体实施策略

### 核心原则

1. **渐进式重构**：自底向上，从无依赖的 leaf 模块开始，始终保持可编译状态
2. **Sans-I/O 架构**：参考 omq.rs，将 ZMTP 协议核心与 I/O 运行时完全分离
3. **编译自愈闭环**：利用 Rust 编译器错误栈驱动修复，每个模块翻译后立即编译验证
4. **测试驱动迁移**：每个模块先翻译对应 C++ 测试，再翻译实现代码
5. **unsafe 最小化**：仅限 FFI 边界和零拷贝热路径，目标 < 10%

### 项目命名与结构

```
libzmq_rust/                        # 顶层 workspace
├── Cargo.toml                      # workspace 清单
├── zmq-core/                       # Sans-I/O ZMTP 协议核心
│   ├── src/
│   │   ├── lib.rs
│   │   ├── constants.rs            # ZMTP 常量、版本号
│   │   ├── error.rs                # ZmqError 枚举
│   │   ├── message.rs              # ZmqMessage（替代 msg_t）
│   │   ├── socket_type.rs          # SocketType 枚举（19 种）
│   │   ├── codec/                  # ZMTP 编码/解码（协议层）
│   │   │   ├── mod.rs
│   │   │   ├── greeting.rs         # ZMTP 握手
│   │   │   ├── command.rs          # ZMTP 命令（READY, SUBSCRIBE 等）
│   │   │   ├── encoder.rs          # 帧编码器
│   │   │   ├── decoder.rs          # 帧解码器
│   │   │   └── mechanism.rs        # 安全机制 trait
│   │   ├── security/               # 安全机制
│   │   │   ├── mod.rs
│   │   │   ├── null.rs             # NULL 机制
│   │   │   ├── plain.rs            # PLAIN 机制
│   │   │   ├── curve.rs            # CURVE 机制（需 libsodium）
│   │   │   └── zap.rs              # ZAP 认证协议
│   │   ├── data_structures/        # 内部数据结构
│   │   │   ├── mod.rs
│   │   │   ├── yqueue.rs           # 无锁队列
│   │   │   ├── ypipe.rs            # 无锁管道
│   │   │   ├── array.rs            # O(1) 删除索引数组
│   │   │   ├── trie.rs             # 订阅匹配 trie
│   │   │   ├── radix_tree.rs       # 基数树订阅
│   │   │   ├── fair_queue.rs       # 公平队列
│   │   │   ├── load_balancer.rs    # 负载均衡器
│   │   │   └── distribution.rs     # 分发器（dist_t）
│   │   └── socket/                 # Socket 行为 trait（纯协议，无 I/O）
│   │       ├── mod.rs
│   │       ├── base.rs             # Socket 基础 trait
│   │       ├── req.rs
│   │       ├── rep.rs
│   │       ├── dealer.rs
│   │       ├── router.rs
│   │       ├── pub.rs
│   │       ├── sub.rs
│   │       ├── xpub.rs
│   │       ├── xsub.rs
│   │       ├── push.rs
│   │       ├── pull.rs
│   │       ├── pair.rs
│   │       └── ...（其他 socket 类型）
│   └── tests/                      # 单元测试（对应原 unittests/）
├── zmq-transport/                  # 传输层（与 I/O 运行时绑定）
│   ├── src/
│   │   ├── lib.rs
│   │   ├── tcp.rs                  # TCP 传输
│   │   ├── ipc.rs                  # Unix domain socket 传输
│   │   ├── inproc.rs               # 进程内传输
│   │   └── endpoint.rs             # 端点解析
│   └── tests/
├── zmq-runtime/                    # 异步运行时抽象
│   ├── src/
│   │   ├── lib.rs
│   │   ├── reactor.rs              # 事件循环 / I/O 线程
│   │   ├── poller.rs               # 跨平台 I/O 多路复用（基于 mio）
│   │   ├── signaler.rs             # 线程间唤醒
│   │   └── thread.rs               # 线程池管理
│   └── tests/
├── zmq-context/                    # 上下文与对外 API
│   ├── src/
│   │   ├── lib.rs
│   │   ├── context.rs              # ZContext（替代 ctx_t）
│   │   ├── socket.rs               # ZSocket（替代 socket_base_t，对外 API）
│   │   ├── options.rs              # Socket 选项
│   │   └── monitor.rs              # Socket 监控事件
│   └── tests/
├── zmq-ffi/                        # C API 兼容层（如果需要）
│   └── src/
│       └── lib.rs                  # #[no_mangle] extern "C" 函数
├── benches/                        # 性能基准（对应原 perf/）
│   ├── inproc_lat.rs
│   ├── inproc_thr.rs
│   ├── local_lat.rs
│   ├── local_thr.rs
│   ├── remote_lat.rs
│   ├── remote_thr.rs
│   └── proxy_thr.rs
├── integration-tests/              # 集成测试（对应原 tests/）
│   ├── test_req_rep.rs
│   ├── test_pub_sub.rs
│   ├── test_push_pull.rs
│   └── ...（对应 142 个原测试文件）
└── examples/                       # 使用示例
    ├── weather_server.rs
    ├── weather_client.rs
    ├── task_ventilator.rs
    └── ...
```

---

## 三、分阶段实施计划

### 第 0 阶段：环境搭建与参考项目研读（1-2 天）

**目标**：建立完整的技术基础，深入理解参考实现。

| 步骤 | 内容 |
|------|------|
| 0.1 | clone 并编译运行 zmq.rs，理解其架构 |
| 0.2 | clone 并编译运行 rzmq，特别关注 io_uring 加速路径 |
| 0.3 | 研究 omq.rs 的 sans-I/O 核心设计（`omq-proto` crate） |
| 0.4 | 搭建 Rust workspace 骨架，配置 CI |
| 0.5 | 配置 benchmark 框架（criterion），建立 C++ 基线数据 |

**产出**：Rust workspace 骨架 + C++ 性能基线 CSV

---

### 第 1 阶段：基础数据结构（Sans-I/O，零依赖）（2-3 天）

**目标**：翻译 libzmq 最底层的无锁数据结构，这是整个系统的基石。

**迁移清单**（对应 `src/` 中的文件）：

| 原 C++ 文件 | Rust 模块 | 行数 | unsafe 预期 | 优先级 |
|------------|-----------|------|-------------|--------|
| `yqueue.hpp` | `data_structures/yqueue.rs` | ~188 | < 5% | P0 |
| `ypipe.hpp` | `data_structures/ypipe.rs` | ~120 | < 5% | P0 |
| `ypipe_conflate.hpp` | `data_structures/ypipe_conflate.rs` | ~86 | < 5% | P0 |
| `array.hpp` | `data_structures/array.rs` | ~164 | 0% | P0 |
| `msg.hpp` + `msg.cpp` | `message.rs` | ~200 | < 10% | P0 |
| `blob.hpp` | `data_structures/blob.rs` | ~80 | 0% | P1 |
| `dbuffer.hpp` | `data_structures/dbuffer.rs` | ~72 | 0% | P1 |
| `atomic_counter.hpp` | `data_structures/atomic.rs` | ~60 | < 5% | P1 |
| `atomic_ptr.hpp` | `data_structures/atomic.rs` | ~100 | < 10% | P1 |

**自愈闭环**：
- 每个数据结构翻译后立即编写 Rust 单元测试（对应 `unittests/unittest_ypipe.cpp` 等）
- 运行 `cargo test` 和 `cargo miri test`（Miri 检测 UB）
- 如编译失败，根据 error stack 逐条修复，直到通过

**产出**：zmq-core data_structures 模块 + 全部单元测试通过

---

### 第 2 阶段：ZMTP 协议编码/解码（Sans-I/O）（3-4 天）

**目标**：实现与 I/O 无关的 ZMTP 3.x 协议核心。

| 原 C++ 文件 | Rust 模块 | 说明 |
|------------|-----------|------|
| `encoder.hpp` | `codec/encoder.rs` | ZMTP 帧编码（状态机） |
| `decoder.hpp` | `codec/decoder.rs` | ZMTP 帧解码（状态机） |
| `i_encoder.hpp` / `i_decoder.hpp` | `codec/mod.rs` | 编解码 trait 定义 |
| `command.hpp` | `codec/command.rs` | ZMTP 命令定义 |
| `zmtp_engine.hpp` + `.cpp` | 暂不翻译（包含 I/O，第 4 阶段处理） | — |

**关键设计决策**：采用 sans-I/O 模式 — 编解码器接收 `&[u8]` 输入，产出结构化事件枚举，不触碰任何 socket 或 async 代码。这使得：
- 编解码器可被任何 I/O 后端复用（Tokio / async-std / 自定义 poller）
- 可对协议层进行纯函数式单元测试（输入字节 → 期望事件序列）

**产出**：zmq-core codec 模块 + 协议模糊测试

---

### 第 3 阶段：传输层（3-4 天）

**目标**：实现跨平台传输抽象。

| 原 C++ 文件 | Rust 模块 | 说明 |
|------------|-----------|------|
| `tcp_listener.hpp/.cpp` + `tcp_connecter.hpp/.cpp` | `zmq-transport/tcp.rs` | TCP 传输 |
| `ipc_listener.hpp/.cpp` + `ipc_connecter.hpp/.cpp` | `zmq-transport/ipc.rs` | Unix domain socket |
| `ip.hpp/.cpp` + `ip_resolver.hpp/.cpp` | `zmq-transport/endpoint.rs` | 地址解析 |
| `tcp_address.hpp/.cpp` + `ipc_address.hpp/.cpp` | 合并到对应传输模块 | — |
| `stream_engine_base.hpp/.cpp` + `stream_engine.hpp/.cpp` | 暂不翻译（第 4 阶段） | — |

**关键技术选型**：
- 异步 I/O：基于 **Tokio**（生态最成熟、zmq.rs 和 rzmq 的共同选择）
- 地址解析：`socket2` crate + 手动实现 `tcp://` `ipc://` 协议解析
- **暂不实现**：TIPC、VMCI、VSOCK、PGM、NORM、WebSocket（按需扩展）

**inproc 传输**：进程内通信，直接使用 `tokio::sync::mpsc` 或基于 ypipe 的 zero-copy 通道。

**产出**：zmq-transport crate + TCP/IPC/inproc 传输 + 地址解析单元测试

---

### 第 4 阶段：引擎与会话层（3-4 天）

**目标**：实现连接生命周期管理和 ZMTP 协议引擎。

| 原 C++ 文件 | Rust 模块 |
|------------|-----------|
| `stream_engine_base.hpp/.cpp` + `stream_engine.hpp/.cpp` + `zmtp_engine.hpp/.cpp` | `zmq-core/engine.rs` |
| `session_base.hpp/.cpp` | `zmq-core/session.rs` |
| `pipe.hpp/.cpp` | `zmq-core/pipe.rs` |
| `io_thread.hpp/.cpp` | `zmq-runtime/reactor.rs` |
| `io_object.hpp/.cpp` | 合并到 engine 模块 |
| `mailbox.hpp/.cpp` + `mailbox_safe.hpp/.cpp` | `zmq-core/mailbox.rs` |
| `signaler.hpp/.cpp` | `zmq-runtime/signaler.rs` |

**关键设计**：
- `StreamEngine`：负责 ZMTP 握手 + 消息帧的读写，持有 `ZmqCodec` 实例
- `Session`：管理一个 peer 连接，持有 `StreamEngine`，与 `Socket` 通过 `Pipe` 通信
- `Pipe`：基于 ypipe 的双向通道，连接 Socket ↔ Session
- 对象生命周期：用 Rust 的 `Drop` + `Arc` 替代 C++ 的 `own_t` 父子 shutdown 握手

**产出**：引擎/会话层 + 多线程连接测试

---

### 第 5 阶段：Socket 类型实现（5-6 天）

**目标**：实现全部 19 种 socket 类型的行为逻辑。

**翻译顺序**（从简单到复杂）：

| 批次 | Socket 类型 | 复杂度 | 说明 |
|------|------------|--------|------|
| 第 1 批 | PUSH, PULL, PAIR | 低 | 单一管道，无路由逻辑 |
| 第 2 批 | PUB, SUB | 中 | 订阅匹配（trie） + 分发 |
| 第 3 批 | REQ, REP, DEALER, ROUTER | 高 | 路由 ID、请求-响应状态机 |
| 第 4 批 | XPUB, XSUB | 高 | 订阅转发 |
| 第 5 批 | STREAM | 中 | 原始 TCP 流 |
| 第 6 批 | SERVER, CLIENT | 中 | 草案 API，简化连接管理 |
| 第 7 批 | RADIO, DISH, GATHER, SCATTER, DGRAM, PEER, CHANNEL | 中低 | 草案 API |

**每个 socket 类型的翻译流程**：
1. 编写 Rust 单元测试（翻译对应的 C++ 测试文件）
2. 实现 socket trait（`xsend`, `xrecv`, `xhas_in`, `xhas_out`, `xpipe_terminated`）
3. `cargo test` → 根据 error stack 自愈 → 循环直到全部通过

**产出**：19 种 socket 类型 + 对应单元测试

---

### 第 6 阶段：上下文 API 与安全机制（3-4 天）

**目标**：实现对外 API 和安全认证。

| 原 C++ 文件 | Rust 模块 |
|------------|-----------|
| `ctx.hpp/.cpp` | `zmq-context/context.rs` |
| `socket_base.hpp/.cpp` | `zmq-context/socket.rs` |
| `options.hpp/.cpp` | `zmq-context/options.rs` |
| `null_mechanism.hpp/.cpp` | `zmq-core/security/null.rs` |
| `plain_client.hpp/.cpp` + `plain_server.hpp/.cpp` | `zmq-core/security/plain.rs` |
| `curve_client.hpp/.cpp` + `curve_server.hpp/.cpp` | `zmq-core/security/curve.rs` |
| `zap_client.hpp/.cpp` | `zmq-core/security/zap.rs` |

**安全机制依赖**：
- CURVE 需要 `libsodium` 或 `sodiumoxide` crate
- GSSAPI 可暂不实现（需 Kerberos，平台兼容性差）

**API 设计**：
```rust
// 对标原 C API
let ctx = ZContext::new();
let socket = ctx.socket(SocketType::REQ)?;
socket.connect("tcp://localhost:5555")?;
socket.send("Hello".into(), 0)?;
let msg = socket.recv(0)?;
```

**产出**：完整对外的 Rust API + C FFI 兼容层（`zmq-ffi` crate，可选）

---

### 第 7 阶段：全量测试迁移（5-7 天）

**目标**：将 142 个 C++ 集成测试 + 7 个单元测试 + 8 个性能基准全部翻译为 Rust。

**测试框架**：使用 Rust 标准 `#[test]` + `rstest`（参数化测试）替代 Unity 框架。

**迁移策略**：
- 按测试模块分类，优先翻译高覆盖率的测试（REQ/REP、PUB/SUB、PUSH/PULL）
- 每个测试文件对应一个 `#[cfg(test)]` 模块
- 使用 `test-context` 模式替代 C++ 的 `SETUP_TEARDOWN_TESTCONTEXT` 宏

**测试辅助工具**：
```rust
// 对标原 testutil 的 Rust 版本
struct TestContext { ctx: ZContext }
impl TestContext {
    fn socket(&self, typ: SocketType) -> ZSocket { ... }
    fn bounce(&self, server: &ZSocket, client: &ZSocket) { ... }
}
```

**产出**：142+ 集成测试全部通过

---

### 第 8 阶段：性能对齐与优化（4-5 天）

**目标**：Rust 版本性能 ≥ C++ 版本 95%。

**优化路径**：

| 优化项 | 预期提升 | 参考来源 |
|--------|----------|----------|
| Sans-I/O 热路径（无 trait 对象、静态分发） | 5-10% | omq.rs 设计 |
| 零拷贝消息传递（`Bytes` 引用计数） | 10-20% | zmq.rs message.rs |
| 批量写入（writev / sendmmsg） | 10-15% | zmq.rs write_queue.rs |
| TCP_NODELAY + TCP_CORK（Linux） | 5% | rzmq 参考 |
| 无锁 SPSC 队列（crossbeam / 自研） | 5-10% | ypipe 重写 |
| io_uring 后端（Linux，可选） | 20-30% | rzmq io_uring_backend |

**基准测试**：
1. 建立 C++ 基线（运行原 `perf/` 下的所有 benchmark）
2. 在相同硬件上运行 Rust benchmark（criterion）
3. 对比 inproc_lat, inproc_thr, local_lat, local_thr, remote_lat, remote_thr, proxy_thr
4. 逐项优化直到所有指标 ≥ 95%

**产出**：性能报告（Rust vs C++ 逐项对比）

---

### 第 9 阶段：unsafe 审计与清理（1-2 天）

**目标**：unsafe 代码占比 < 10%。

**审计清单**：
1. 使用 `cargo geiger` 或 `#![deny(unsafe_code)]` 扫描全部 unsafe 使用
2. 为每个 `unsafe` 块添加 `// SAFETY:` 注释，说明不变量
3. 用 `miri` 运行全部测试，检测未定义行为
4. 将可以安全化的 `unsafe` 重构为 safe Rust

**当前预期 unsafe 分布**：
- 无锁数据结构（yqueue/ypipe）：~5%
- 零拷贝消息缓冲（msg_t 等价实现）：~3%
- FFI 边界（libsodium 集成、C API 兼容层）：~1%
- 合计预期：~8-9%

**产出**：unsafe 审计报告

---

## 四、关键技术决策

### 4.1 架构：Sans-I/O ZMTP 核心（参考 omq.rs）

将 ZMTP 协议处理（编解码、状态机、命令）与 I/O（TCP socket、事件循环）完全分离。核心 crate `zmq-core` 不依赖任何 async 运行时，零 I/O 依赖。

```rust
// 协议核心（zmq-core）：纯数据处理，任何运行时可用
impl ZmqCodec {
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError>;
    fn encode(&mut self, cmd: ZmqCommand) -> Result<Vec<u8>, ZmqError>;
}

// 传输层（zmq-transport）：对接 Tokio / mio
impl TcpTransport {
    async fn connect(&self, endpoint: &Endpoint) -> Result<FramedIo, ZmqError>;
}
```

### 4.2 异步运行时：Tokio（默认）

- Tokio 生态最成熟，是 zmq.rs 和 rzmq 的共同选择
- 平台支持最广（Linux / macOS / Windows）
- 后续可选支持 io_uring（类似 rzmq）

### 4.3 无锁数据结构：自定义 + crossbeam

- `yqueue`/`ypipe`：自定义实现（参考原 C++ 逻辑，用 Rust `UnsafeCell` + `AtomicPtr`）
- 通用场景：使用 `crossbeam` channel + `parking_lot` Mutex

### 4.4 测试框架：Rust 标准 `#[test]` + `rstest`

- 单元测试：`#[test]` + `#[cfg(test)]` 模块
- 参数化测试：`rstest` crate（替代 C++ 的 `def_test_spec_*` 宏）
- 模糊测试：`cargo fuzz`（libfuzzer）
- 性能测试：`criterion` benchmark

---

## 五、编译自愈闭环机制

### 实现方式

1. **模块翻译** → `cargo build 2> errors.txt`
2. **错误解析** → 提取 Rust 编译器 error code + span
3. **自动修复** → 根据错误类型应用修复策略：
   - `E0308`（类型不匹配）→ 检查 C++ 原类型，修正 Rust 签名
   - `E0599`（方法未找到）→ 检查 trait bound，补充实现
   - `E0382`（所有权移动）→ 添加 `.clone()` 或改为引用
   - `E0502`（借用冲突）→ 重构所有权结构
   - `E0609`（字段不存在）→ 对照 C++ 类成员，补充 struct 字段
4. **循环** → `cargo build` → 错误 → 修复 → 直到 compile success
5. **测试验证** → `cargo test` → 失败 → 修复 → 循环

### 技术实现

可以在 `build.rs` 或外部脚本中实现自动化流程：
```rust
// build.rs 伪代码
loop {
    let output = Command::new("cargo").args(["build"]).output()?;
    if output.status.success() { break; }
    let errors = parse_rustc_errors(&output.stderr);
    for error in errors {
        apply_fix(error)?;
    }
}
```

---

## 六、风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| C++ 模板/宏难以等价翻译 | 中 | 使用 Rust trait + 泛型替代模板；macro_rules! 替代 C 宏 |
| Windows epoll 兼容性 | 中 | 使用 `mio`（已封装 wepoll）+ 条件编译 |
| CURVE 加密性能差距 | 高 | 复用 `sodiumoxide` crate（与 libsodium 同源，性能相同） |
| 零拷贝消息零开销难以保证 | 高 | 使用 `bytes::Bytes`（引用计数）+ 自定义 VSM 优化 |
| io_uring 仅在 Linux 可用 | 低 | 仅作为可选 feature，默认使用 Tokio epoll |
| 50k 行代码量过大 | 中 | 渐进式迁移，从最简路径开始，每阶段产出可运行代码 |

---

## 七、里程碑与时间线

| 里程碑 | 阶段 | 预计时间 | 关键产出 |
|--------|------|----------|----------|
| M1 | 0-2 | 第 1 周 | 环境 + 数据结构 + 协议编解码完成 |
| M2 | 3-4 | 第 2 周 | 传输层 + 引擎/会话层完成 |
| M3 | 5-6 | 第 3 周 | 核心 Socket 类型（REQ/REP/PUB/SUB/PUSH/PULL）可用 |
| M4 | 7 | 第 4 周 | 全部测试迁移完成，集成测试通过 |
| M5 | 8-9 | 第 5 周 | 性能达标（≥95%）+ unsafe 审计通过 |

总计预估：**5 周**（可根据实际进度调整）

---

## 八、参考资源汇总

### 开源项目
- [zmq.rs (官方)](https://github.com/zeromq/zmq.rs) — 最权威的纯 Rust 参考实现
- [rzmq](https://github.com/excsn/rzmq) — 最活跃的纯 Rust 实现，io_uring 加速
- [omq.rs](https://github.com/Paddor/omq.rs) — Sans-I/O 架构标杆
- [rust-zmq](https://github.com/zeromq/rust-zmq) — C 绑定（用于对比 C API 行为）

### 架构文档
- [libzmq Internal Architecture](http://wiki.zeromq.org/whitepapers:architecture)
- [libzmq DeepWiki](https://deepwiki.com/zeromq/libzmq)

### 工具
- [C2Rust](https://c2rust.com/) — C→Rust 语法转译（参考用，不直接用于 C++）
- [mio](https://github.com/tokio-rs/mio) — 跨平台 I/O 多路复用

### 学术论文
- [EvoC2Rust: Skeleton-Guided C-to-Rust Translation](https://arxiv.org/abs/2508.04295)
- [His2Trans: Historical Knowledge Reuse for C-to-Rust](https://arxiv.org/abs/2603.02617)
