# CLAUDE.md — libzmq Rust 重写项目

## 文档索引

所有设计文档和进展记录在 [`docs/`](docs/) 目录下：

| 文件 | 内容 |
|------|------|
| [docs/01-总体设计.md](docs/01-总体设计.md) | 完整设计文档 — 9 阶段方案、架构图、模块接口定义 |
| [docs/02-进展日志.md](docs/02-进展日志.md) | 每次会话的进展、踩坑记录、下一步计划 |
| [docs/03-测试清单.md](docs/03-测试清单.md) | **143 个集成测试 + 7 个单元测试的完整迁移清单** |

每次完成工作后，必须更新 `docs/02-进展日志.md`。

## 项目定位
将 libzmq (C++ ZeroMQ 核心库, ~50k 行) 用纯 Rust 重写，要求：
- 渐进式重构，保持模块间调用关系
- 编译自愈闭环（根据 Rust error stack 自动修复）
- 语义等价 + 全路径单元测试
- unsafe < 10%，性能 ≥ C++ 95%

## 当前状态
- **全 workspace**: 209 passed, 0 failed, 205 ignored（2026-06-12）
- **工作 socket**: PAIR/REQ/REP/PUSH/PULL — inproc 传输正常
- **桩 socket (xrecv 返回 NoMessage)**: ROUTER、DEALER
- **未实现 socket**: PUB/SUB/XPUB/XSUB/STREAM/SCATTER/GATHER 等草案 API
- **关键限制**: 仅 inproc 传输可用，TCP/IPC/UDP 传输尚未集成 session

## 环境约束（重要！避免踩坑）

### Rust 工具链
- **必须使用 GNU toolchain**：`stable-x86_64-pc-windows-gnu`
- MSVC toolchain 不可用（本机没有 Visual Studio C++ build tools，link.exe 报错）
- 安装命令：`rustup default stable-x86_64-pc-windows-gnu`
- cargo/rustc 路径：`$HOME/.cargo/bin/`，使用时需 `export PATH="$HOME/.cargo/bin:$PATH"`

### Cargo 依赖注意事项
- workspace 级别的 `[workspace.dependencies]` 中的依赖**不能**标记 `optional = true`
- 可选依赖必须在具体 crate 的 `Cargo.toml` 中声明
- crate 名使用下划线：`win_uds`（非 `win-uds`），`parking_lot`（非 `parking-lot`）
- `sodiumoxide` 用于 CURVE 加密，是可选依赖，仅在 `zmq-core` 中以 feature gate 引入

## 项目结构（关键！不要搞错模块归属）

```
libzmq_rust/
├── zmq-core/          — Sans-I/O 协议核心（零 I/O 依赖，不得引用 tokio/mio）
├── zmq-transport/     — 传输层（TCP/IPC/inproc），绑定 Tokio
├── zmq-runtime/       — 异步运行时抽象（reactor/poller/signaler/thread_pool）
├── zmq-context/       — 对外公开 API（ZContext/ZSocket/options/monitor）
└── zmq-ffi/           — C 兼容层（#[no_mangle] extern "C"）
```

### 模块依赖方向（单向，不得反向）
```
zmq-ffi → zmq-context → zmq-transport + zmq-runtime → zmq-core
```

### zmq-core 内部模块结构
- `codec/` — ZMTP 编解码（ZmqDecoder/ZmqEncoder/Greeting/Command/Framing/Mechanism）
- `data_structures/` — 无锁数据结构（yqueue/ypipe/array/trie/radix_tree/fair_queue/load_balancer）
- `security/` — 安全机制（null/plain/curve/zap）
- `socket/` — 19 种 socket 类型的行为逻辑（base/routing/pub_socket/sub_socket/req/rep/...）
- 顶层：`message.rs`, `error.rs`, `socket_type.rs`, `constants.rs`, `pipe.rs`, `engine.rs`, `session.rs`, `mailbox.rs`

### 重要符号位置（避免 E0432 错误）
- `ZmqEvent` 定义在 `zmq_core::codec::decoder::ZmqEvent`（非 `codec::ZmqEvent`）
- `ZmqDecoder` 也在 `zmq_core::codec::decoder::ZmqDecoder`
- `ZmqEncoder` 在 `zmq_core::codec::encoder::ZmqEncoder`
- `Command` 在 `zmq_core::codec::command::Command`（非 `ZmqCommand`）
- `Greeting` 在 `zmq_core::codec::greeting::Greeting`

## 参考项目（设计决策依据）

| 项目 | 用途 |
|------|------|
| [zmq.rs](https://github.com/zeromq/zmq.rs) | 官方纯 Rust 实现，v0.6.0，参考 GenericSocketBackend/FairQueue/WriteQueue |
| [rzmq](https://github.com/excsn/rzmq) | 最活跃实现，v0.5.18，参考 io_uring 加速、CURVE 集成 |
| [omq.rs](https://github.com/Paddor/omq.rs) | Sans-I/O 架构标杆，参考 `omq-proto` crate 设计 |
| [rust-zmq](https://github.com/zeromq/rust-zmq) | C 绑定包装，仅用于对比 C API 行为 |

## 架构核心原则（不得违反）

1. **Sans-I/O**：协议处理与 I/O 完全分离。`zmq-core` 不依赖任何 async 运行时
2. **渐进式**：始终从无依赖的 leaf 模块开始翻译，保持项目可编译
3. **测试驱动**：每个模块先翻译对应 C++ 测试文件，再翻译实现
4. **unsafe 最小化**：仅限无锁数据结构和 FFI 边界，每处 unsafe 须有 `// SAFETY:` 注释

## 测试对齐规则（严格执行）

**每个 Phase 完成的验收标准：对应 C++ 测试必须 1:1 翻译为 Rust 测试并通过。**

| C++ 测试文件 | Rust 测试位置 | 所属 Phase |
|-------------|--------------|-----------|
| `unittests/unittest_ypipe.cpp` | `data_structures::ypipe::tests` | Phase 1 ✅ |
| `unittests/unittest_mtrie.cpp` | `data_structures::trie::tests` | Phase 1 ✅ |
| `unittests/unittest_radix_tree.cpp` | `data_structures::radix_tree::tests` | Phase 1 ✅ |
| `unittests/unittest_poller.cpp` | 待定 | Phase 4 |
| `unittests/unittest_ip_resolver.cpp` | `zmq-transport::endpoint::tests` | Phase 3 |
| `unittests/unittest_udp_address.cpp` | 待定 | Phase 3 |
| `unittests/unittest_curve_encoding.cpp` | `security::curve::tests` | Phase 6 |
| `tests/test_*.cpp` (142 个) | `tests/` 目录 | Phase 7 |

**对齐要求**：
- 每个 C++ `TEST_ASSERT_*` 宏必须有对应的 Rust `assert_*`
- C++ 的 `SETUP_TEARDOWN_TESTCONTEXT` → Rust 的 `TestContext` 结构体
- C++ 的 `bounce()` / `s_send_seq()` 等 helper → Rust 的同名 helper 函数
- 测试函数命名保持一致：`test_foo_bar` → `test_foo_bar`

## C++ → Rust 映射速查

| C++ 类/文件 | Rust 模块 | 阶段 |
|------------|-----------|------|
| `yqueue.hpp` | `data_structures::yqueue` | Phase 1 |
| `ypipe.hpp` | `data_structures::ypipe` | Phase 1 |
| `msg_t` | `ZmqMessage` (已实现) | Phase 1 |
| `array.hpp` | `data_structures::array` | Phase 1 |
| `fq.hpp` / `lb.hpp` / `dist.hpp` | `fair_queue` / `load_balancer` / `distribution` | Phase 1 |
| `encoder.hpp` / `decoder.hpp` | `codec::encoder` / `codec::decoder` | Phase 2 |
| `stream_engine.hpp` | `engine::ZmtpEngine` | Phase 4 |
| `session_base.hpp` | `session::Session` | Phase 4 |
| `object_t` / `own_t` | Rust `Arc` + `Drop` 替代 | Phase 4 |
| `ctx_t` | `zmq_context::ZContext` | Phase 6 |
| `socket_base_t` | `zmq_context::ZSocket` | Phase 6 |
| 各 socket 类型 | `zmq_core::socket::*` (19 种) | Phase 5 |

## Pipe 架构（重要！理解和修改前必读）

`Pipe::new_pair()` 创建两个独立 Pipe，共享 `Arc<Mutex<YPipe>>` 底层队列：

```
A.to_session = a_to_b (A 发 → B 收)
A.to_socket = b_to_a (B 发 → A 收)  
B.to_session = b_to_a (B 发 → A 收)
B.to_socket = a_to_b (A 发 → B 收)
```

- `write_to_session()` → 写 `to_session` 队列（本地发往对端）
- `read_from_session()` → 读 `to_socket` 队列（对端发往本地）
- 所有 socket 的 xrecv 使用 `read_from_session()`（读 to_socket）
- 所有 socket 的 xsend 使用 `write_to_session()`（写 to_session）

**重要踩坑记录**：
1. **Pipe ID 必须全局唯一** — `Pipe::new_pair()` 使用 `AtomicUsize` 全局计数器。所有 pipe pair 使用相同 ID(0/1) 会导致 HashMap 只保留最后一个 pipe。
2. **REQ option 需传播到 inner socket** — `set_req_relaxed()` / `set_req_correlate()` 不能只存 ZSocket 的 options，必须通过 Socket trait 方法写入 inner socket。
3. **DEALER/ROUTER xrecv 为桩** — 目前返回 NoMessage，需要实现完整接收逻辑。

## 常用命令

```bash
# 编译检查（开发时用 check 比 build 快）
export PATH="$HOME/.cargo/bin:$PATH"
cd libzmq_rust
cargo check

# 运行全部测试
cargo test

# 运行特定 crate 的测试
cargo test -p zmq-core

# 运行单个测试
cargo test --test test_spec_req

# miri 检测未定义行为（需要 nightly）
cargo +nightly miri test -p zmq-core
```

## 文件命名约定
- Rust 模块文件使用**小写下划线**：`pub_socket.rs`, `sub_socket.rs`（保留字冲突时）
- 测试文件在 `tests/` 目录下，与 C++ 测试 1:1 对应
- 所有文档在 `docs/` 目录下，按编号命名
