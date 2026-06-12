# 问题 01：VSM 优化性能对比不准确

## 严重程度
🔴 严重

## 问题描述

性能测试中的 VSM（Very Small Message）优化对比存在不公平性，无法证明 Rust 实现真正优于 C++。

## 问题详情

### 当前测试方式

```markdown
> 说明：C++ `inproc_thr` 测量的是完整 PUSH→PULL socket 路径
> （含 socket 层、lb_t/fq_t 调度器），Rust 测量裸 ypipe。
```

### 对比数据

| 基准 | C++ | Rust | 比率 |
|------|-----|------|------|
| inproc_thr (30B VSM) | 11,940,298 msg/s | 14,454,836 msg/s | 121% |

### 问题分析

1. **测量路径不一致**
   - C++：完整 PUSH→PULL 路径（含 socket 层、lb_t/fq_t 调度器）
   - Rust：裸 ypipe（无调度器开销）

2. **无法证明的结论**
   - README 声称 "Rust 吞吐超过 C++"
   - 实际上是裸数据结构 vs 完整路径，不公平对比

3. **误导性**
   - 可能让用户误以为 Rust 实现整体性能更优
   - 实际上只证明了 ypipe 数据结构的性能

## 影响

- 性能报告不可信
- 无法验证完整路径的性能
- 可能误导用户选择

## 改进建议

### 方案 A：测量相同路径

```rust
// Rust 也应测量完整的 PUSH→PULL 路径
#[test]
fn test_push_pull_throughput() {
    let ctx = ZContext::new();
    let push = ctx.socket(SocketType::PUSH).unwrap();
    let pull = ctx.socket(SocketType::PULL).unwrap();

    push.bind("inproc://test").unwrap();
    pull.connect("inproc://test").unwrap();

    // 测量完整路径的吞吐量
    let start = Instant::now();
    for i in 0..100_000 {
        push.send(&format!("msg {}", i), 0).unwrap();
    }
    let elapsed = start.elapsed();

    println!("Throughput: {} msg/s", 100_000 / elapsed.as_secs_f64());
}
```

### 方案 B：明确说明测试范围

```markdown
### Rust 性能对比

| 基准 | C++ | Rust | 比率 | 说明 |
|------|-----|------|------|------|
| inproc_thr (30B VSM) | 11,940,298 msg/s | 14,454,836 msg/s | 121% | ⚠️ Rust 仅测量裸 ypipe |
| radix_tree | 103.0 ns | 55.6 ns | 185% | ✅ 相同基准 |

> **注意**：inproc_thr 测试中，Rust 测量的是裸 ypipe 数据结构，
> 而 C++ 测量的是完整的 PUSH→PULL 路径（含调度器）。
> 两者测量范围不同，不可直接对比。
```

### 方案 C：添加完整路径基准测试

```rust
// benches/throughput.rs
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_push_pull(c: &mut Criterion) {
    c.bench_function("push_pull_30b", |b| {
        let ctx = ZContext::new();
        let push = ctx.socket(SocketType::PUSH).unwrap();
        let pull = ctx.socket(SocketType::PULL).unwrap();
        push.bind("inproc://bench").unwrap();
        pull.connect("inproc://bench").unwrap();

        b.iter(|| {
            push.send(&[0u8; 30], 0).unwrap();
            pull.recv_bytes().unwrap();
        });
    });
}

criterion_group!(benches, bench_push_pull);
criterion_main!(benches);
```

## 优先级

**P0（必须修复）**

- 性能报告的可信度直接影响项目价值
- 需要提供公平的对比数据

## 相关文件

- `README.md` 第 167-169 行
- `libzmq_rust/perf/inproc_thr.rs`
