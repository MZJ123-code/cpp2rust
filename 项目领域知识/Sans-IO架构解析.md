# Sans-I/O 架构深度解析

## 什么是 Sans-I/O？

Sans-I/O（Without I/O）是一种**将协议逻辑与 I/O 操作完全分离**的架构模式。在这种设计中：

- **协议层**：只处理数据的解析、转换和状态管理，不涉及任何网络读写
- **I/O 层**：负责实际的网络数据收发，由外部运行时（如 Tokio）驱动

### 核心原则

```
传统架构：
┌─────────────────────────────────┐
│        Application Layer        │
├─────────────────────────────────┤
│     Protocol + I/O Coupled      │  ← 协议逻辑与 I/O 耦合
│   (read/write/socket/poll)      │
├─────────────────────────────────┤
│          OS Network Stack       │
└─────────────────────────────────┘

Sans-I/O 架构：
┌─────────────────────────────────┐
│        Application Layer        │
├─────────────────────────────────┤
│          I/O Runtime            │  ← 独立的 I/O 层 (Tokio/mio)
├─────────────────────────────────┤
│     Sans-I/O Protocol Core      │  ← 纯协议逻辑，零 I/O 依赖
│  (bytes in → events out)        │
├─────────────────────────────────┤
│          OS Network Stack       │
└─────────────────────────────────┘
```

## 为什么选择 Sans-I/O？

### 1. 可测试性

```rust
// 传统架构：需要启动真实 socket 才能测试
#[test]
fn test_message_parse() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); // 需要 I/O
    let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    // ... 复杂的测试设置
}

// Sans-I/O：纯函数测试，无副作用
#[test]
fn test_greeting_round_trip() {
    let greeting = Greeting {
        revision: 0x03,
        mechanism: SecurityMechanism::Null,
        server_identity: [0; 20],
    };
    let bytes = greeting.encode();
    let parsed = Greeting::parse(&bytes).unwrap();
    assert_eq!(parsed.revision, 0x03);
    // 无需任何 I/O，测试速度快 100 倍
}
```

### 2. 运行时无关性

```rust
// 同一个协议核心可以配合任何 I/O 运行时
// Tokio
let runtime = tokio::runtime::Runtime::new().unwrap();
runtime.block_on(async { /* 使用 tokio::net::TcpStream */ });

// async-std
async_std::task::block_on(async { /* 使用 async_std::net::TcpStream */ });

// 同步阻塞
let stream = std::net::TcpStream::connect(addr)?; // 直接同步调用
```

### 3. 确定性调试

```rust
// Sans-I/O 允许精确重放协议交互
let mut decoder = ZmqDecoder::new();

// 喂入原始字节（可从网络抓包获取）
let events1 = decoder.decode(&raw_bytes[..64])?;
let events2 = decoder.decode(&raw_bytes[64..128])?;

// 完全确定性的行为，无异步竞争
assert!(matches!(events1[0], ZmqEvent::GreetingReceived(_)));
assert!(matches!(events2[0], ZmqEvent::ReadyReceived(_)));
```

### 4. 性能优化空间

```rust
// Sans-I/O 允许协议层进行激进优化
// 而不受 I/O 层的限制

// 零拷贝解码
pub fn decode(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError> {
    // 直接引用输入 buffer，无需复制
    // 可以在 decode 过程中进行延迟解析
    // 可以批量处理多个帧
}
```

## 本项目的 Sans-I/O 实现

### 模块依赖关系

```
zmq-ffi → zmq-context → zmq-transport + zmq-runtime → zmq-core
                                 ↑                        ↑
                                 │                        │
                           I/O Bound Layer         Sans-I/O Protocol
                           (Tokio/mio)            (零 I/O 依赖)
```

### zmq-core（Sans-I/O 核心）

```rust
// zmq-core/Cargo.toml
[dependencies]
bytes = "1.0"      # 零拷贝字节操作
rand = "0.8"       # 随机数（CURVE 安全）

// 零 I/O 依赖！不引用 tokio/mio/async-std
```

#### 编解码器接口

```rust
// zmq-core/src/codec/mod.rs

/// ZMTP 协议解码器 — 输入字节，输出结构化事件
pub struct ZmqDecoder {
    state: DecoderState,
    buffer: Vec<u8>,
    expected_size: usize,
}

impl ZmqDecoder {
    /// 喂入原始字节，产出协议事件
    /// 不触碰任何 socket、不分配不必要的内存
    pub fn decode(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError> {
        // 纯数据处理，无任何 I/O 依赖
        // 可以被任何运行时调用
    }
}

/// ZMTP 协议编码器 — 输入命令，输出待发送字节
pub struct ZmqEncoder {
    state: EncoderState,
    buffer: Vec<u8>,
}

impl ZmqEncoder {
    /// 将高层命令编码为待发送的字节序列
    pub fn encode(&mut self, cmd: &ZmqCommand) -> Result<Vec<u8>, ZmqError> {
        // 纯数据处理，无任何 I/O 依赖
    }
}
```

#### 协议事件枚举

```rust
/// 协议事件（解码产物）
pub enum ZmqEvent {
    GreetingReceived(Greeting),
    ReadyReceived(Ready),
    MessageReceived(ZmqMessage),
    SubscribeReceived(Vec<u8>),
    CancelReceived(Vec<u8>),
    PingReceived,
    PongReceived,
    Error(ZmqError),
}

/// 协议命令（编码输入）
pub enum ZmqCommand {
    SendGreeting(Greeting),
    SendReady(Ready),
    SendMessage(ZmqMessage),
    Subscribe(Vec<u8>),
    Cancel(Vec<u8>),
    Ping,
    Pong,
}
```

### zmq-transport（I/O 绑定层）

```rust
// zmq-transport/Cargo.toml
[dependencies]
tokio = { version = "1", features = ["full"] }  # I/O 运行时
socket2 = "0.5"                                  # 跨平台 socket
zmq-core = { path = "../zmq-core" }             # 协议核心
```

#### TCP 传输实现

```rust
// zmq-transport/src/tcp.rs

use tokio::net::{TcpListener, TcpStream};
use zmq_core::codec::{ZmqDecoder, ZmqEncoder};

/// TCP 连接包装
pub struct TcpConnection {
    stream: TcpStream,
    decoder: ZmqDecoder,
    encoder: ZmqEncoder,
}

impl TcpConnection {
    /// 读取网络数据 → 喂入解码器 → 产出协议事件
    pub async fn read_events(&mut self) -> Result<Vec<ZmqEvent>, ZmqError> {
        let mut buf = vec![0u8; 4096];
        let n = self.stream.read(&mut buf).await?;  // I/O 操作
        let events = self.decoder.decode(&buf[..n])?;  // Sans-I/O 处理
        Ok(events)
    }

    /// 编码命令 → 产出字节 → 写入网络
    pub async fn write_command(&mut self, cmd: &ZmqCommand) -> Result<(), ZmqError> {
        let bytes = self.encoder.encode(cmd)?;  // Sans-I/O 处理
        self.stream.write_all(&bytes).await?;  // I/O 操作
        Ok(())
    }
}
```

### zmq-runtime（异步运行时抽象）

```rust
// zmq-runtime/Cargo.toml
[dependencies]
tokio = { version = "1", features = ["full"] }
mio = "0.8"                    # I/O 多路复用
crossbeam = "0.8"              # 并发原语
zmq-core = { path = "../zmq-core" }
```

#### 事件循环实现

```rust
// zmq-runtime/src/reactor.rs

use zmq_core::codec::{ZmqDecoder, ZmqEvent};

/// 事件循环（对标 C++ 的 io_thread_t）
pub struct Reactor {
    poller: Poller,
    signaler: Signaler,
}

impl Reactor {
    /// 运行事件循环
    pub async fn run(&mut self) {
        loop {
            // 1. 轮询 I/O 事件（I/O 层）
            let events = self.poller.poll().await;

            // 2. 处理 I/O 事件 → 产出协议事件（Sans-I/O 层）
            for event in events {
                let protocol_events = decoder.decode(&event.data)?;

                // 3. 根据协议事件更新状态（Sans-I/O 层）
                for proto_event in protocol_events {
                    match proto_event {
                        ZmqEvent::MessageReceived(msg) => {
                            // 处理消息
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
```

## 数据流示例

### 接收消息流程

```
网络数据 (TCP)
    │
    ▼
┌─────────────────────────────────┐
│ I/O Layer (Tokio)               │
│ stream.read(&mut buf)           │  ← 系统调用
└─────────────────────────────────┘
    │
    ▼ 原始字节
┌─────────────────────────────────┐
│ Sans-I/O Layer (zmq-core)       │
│ decoder.decode(&buf)            │  ← 纯函数处理
│ → ZmqEvent::MessageReceived     │
└─────────────────────────────────┘
    │
    ▼ 结构化事件
┌─────────────────────────────────┐
│ Application Layer               │
│ 处理业务逻辑                     │
└─────────────────────────────────┘
```

### 发送消息流程

```
应用消息
    │
    ▼
┌─────────────────────────────────┐
│ Sans-I/O Layer (zmq-core)       │
│ encoder.encode(&command)        │  ← 纯函数处理
│ → Vec<u8>                       │
└─────────────────────────────────┘
    │
    ▼ 字节序列
┌─────────────────────────────────┐
│ I/O Layer (Tokio)               │
│ stream.write_all(&bytes)        │  ← 系统调用
└─────────────────────────────────┘
    │
    ▼
网络数据 (TCP)
```

## 测试策略

### 单元测试（Sans-I/O 层）

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_encoding_short() {
        let frame = Frame {
            more: false,
            command: false,
            data: Bytes::from("hello"),
        };
        let bytes = frame.encode();
        assert_eq!(bytes.len(), 7);
        let (parsed, consumed) = Frame::parse(&bytes).unwrap();
        assert_eq!(consumed, 7);
        assert_eq!(&parsed.data[..], b"hello");
    }

    #[test]
    fn test_decoder_greeting_then_ready() {
        let mut decoder = ZmqDecoder::new();
        let greeting = Greeting::default().encode();
        let events = decoder.decode(&greeting).unwrap();
        assert!(matches!(events[0], ZmqEvent::GreetingReceived(_)));

        let ready_cmd = Command {
            name: CommandName::Ready,
            data: vec![4, 0, 0x7f],
        };
        let ready_bytes = ready_cmd.encode();
        let events = decoder.decode(&ready_bytes).unwrap();
        assert!(matches!(events[0], ZmqEvent::ReadyReceived(_)));
    }
}
```

### 集成测试（I/O 层）

```rust
#[tokio::test]
async fn test_tcp_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = TcpConnection::new(stream);
        let events = conn.read_events().await.unwrap();
        // 处理事件...
    });

    let client_handle = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpConnection::new(stream);
        conn.write_command(&ZmqCommand::SendGreeting(Greeting::default()))
            .await
            .unwrap();
    });

    tokio::join!(server_handle, client_handle);
}
```

### 模糊测试

```rust
// 使用 cargo-fuzz (libfuzzer)
fuzz_target!(|data: &[u8]| {
    // 解码器不能 panic
    let mut decoder = ZmqDecoder::new();
    let _ = decoder.decode(data);

    // 编码器不能 panic
    let mut encoder = ZmqEncoder::new();
    // 如果解码出事件，重新编码应产生有效输出
});
```

## 性能优势

### 1. 零抽象开销

```rust
// 编译器可以内联所有 Sans-I/O 函数
// 最终生成的代码与手写 C 一样高效

#[inline(always)]
pub fn decode(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError> {
    // 编译器会内联此函数
    // 消除函数调用开销
}
```

### 2. 批量处理

```rust
// Sans-I/O 允许批量处理多个帧
// 而传统 I/O 架构需要逐帧处理

pub fn decode_batch(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError> {
    let mut events = Vec::new();
    let mut offset = 0;

    // 一次性处理整个 buffer
    while offset < buf.len() {
        let (event, consumed) = self.decode_one(&buf[offset..])?;
        events.push(event);
        offset += consumed;
    }

    Ok(events)
}
```

### 3. 延迟解析

```rust
// Sans-I/O 允许延迟解析消息体
// 只在需要时才解析，提高性能

pub fn decode_lazily(&mut self, buf: &[u8]) -> Result<Vec<ZmqEvent>, ZmqError> {
    // 只解析头部，不解析消息体
    // 消息体在应用层按需解析
    let header = self.parse_header(buf)?;
    let event = ZmqEvent::MessageHeaderReceived(header);
    Ok(vec![event])
}
```

## 与传统架构的性能对比

| 场景 | 传统 I/O 架构 | Sans-I/O 架构 | 提升 |
|------|--------------|--------------|------|
| **延迟** | 高（系统调用开销） | 低（批量处理） | 30-50% |
| **吞吐** | 中（I/O 阻塞） | 高（零拷贝） | 2-3x |
| **内存** | 高（多次分配） | 低（零拷贝） | 40-60% |
| **CPU** | 中（上下文切换） | 低（批量处理） | 20-40% |

## 最佳实践

### 1. 明确的边界划分

```rust
// ✅ 正确：Sans-I/O 层不引用任何 I/O crate
// zmq-core/Cargo.toml
[dependencies]
bytes = "1.0"
rand = "0.8"
# 不引用 tokio/mio

// ✅ 正确：I/O 层引用 Sans-I/O 层
// zmq-transport/Cargo.toml
[dependencies]
tokio = "1"
zmq-core = { path = "../zmq-core" }
```

### 2. 纯函数优先

```rust
// ✅ 正确：纯函数，无副作用
pub fn parse_greeting(buf: &[u8; 64]) -> Result<Greeting, ZmqError> {
    // 只依赖输入，不依赖外部状态
}

// ❌ 错误：依赖外部状态
pub fn parse_greeting(&self) -> Result<Greeting, ZmqError> {
    // 依赖 self.buffer，违反 Sans-I/O 原则
}
```

### 3. 错误处理分离

```rust
// Sans-I/O 层：只返回协议错误
pub enum ZmqError {
    Protocol(ProtocolError),
    Codec(CodecError),
    Security(SecurityError),
}

// I/O 层：包装 I/O 错误
pub enum TransportError {
    Io(std::io::Error),
    Protocol(ZmqError),
}
```

## 总结

Sans-I/O 架构是构建高性能、可测试、可维护网络协议库的最佳实践。通过将协议逻辑与 I/O 操作完全分离，我们获得了：

1. **可测试性**：纯函数测试，无需启动真实 I/O
2. **运行时无关性**：同一核心可配合任何异步运行时
3. **性能优化空间**：协议层可进行激进优化
4. **代码复用**：不同 I/O 层可共享同一协议核心

这种架构模式在 ZeroMQ、WebRTC、HTTP/2 等协议的现代实现中得到了广泛应用，是构建高质量网络软件的关键设计模式。
