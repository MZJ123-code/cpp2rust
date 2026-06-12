# 问题 07：缺少 CI/CD 集成

## 严重程度
🟠 中等

## 问题描述

没有 GitHub Actions 或其他 CI/CD 配置，代码质量检查（clippy、rustfmt）未自动化，Miri 测试未在 CI 中运行。

## 问题详情

### 当前状态

- 没有 `.github/workflows/` 目录
- 没有自动化测试
- 没有代码质量检查
- 没有性能回归检测

### 问题分析

1. **代码质量无法保证**
   - 没有自动运行 clippy
   - 没有自动运行 rustfmt
   - 代码风格不一致

2. **测试无法自动运行**
   - 每次提交都需要手动测试
   - 容易遗漏测试
   - 无法保证测试通过

3. **unsafe 代码无法验证**
   - 没有自动运行 Miri
   - unsafe 代码可能有未定义行为
   - 无法保证内存安全

## 影响

- 代码质量不稳定
- 容易引入 bug
- 无法保证安全性

## 改进建议

### 方案 A：添加基础 CI

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    name: Test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust stable
        uses: dtolnay/rust-toolchain@stable

      - name: Cache cargo registry
        uses: actions/cache@v3
        with:
          path: |
            ~/.cargo/registry
            ~/.cargo/git
            target
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}

      - name: Run tests
        run: cargo test --all

  clippy:
    name: Clippy
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy

      - name: Run clippy
        run: cargo clippy --all -- -D warnings

  fmt:
    name: Format
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt

      - name: Check formatting
        run: cargo fmt --check
```

### 方案 B：添加 Miri 检测

```yaml
# .github/workflows/miri.yml
name: Miri

on: [push, pull_request]

jobs:
  miri:
    name: Miri
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust nightly
        uses: dtolnay/rust-toolchain@nightly
        with:
          components: miri

      - name: Run Miri tests
        run: cargo miri test -p zmq-core

      - name: Run Miri with flags
        run: |
          cargo miri test -p zmq-core -- \
            -Zmiri-disable-isolation \
            -Zmiri-symbolic-alignment-check \
            -Zmiri-track-raw-pointers
```

### 方案 C：添加性能基准测试

```yaml
# .github/workflows/benchmark.yml
name: Benchmark

on: [push, pull_request]

jobs:
  benchmark:
    name: Benchmark
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Run benchmarks
        run: cargo bench --bench all 2>&1 | tee output.txt

      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: cargo
          output-file-path: output.txt
          alert-threshold: '110%'
          fail-on-alert: true
```

### 方案 D：添加代码覆盖率

```yaml
# .github/workflows/coverage.yml
name: Coverage

on: [push, pull_request]

jobs:
  coverage:
    name: Coverage
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: llvm-tools-preview

      - name: Install cargo-tarpaulin
        run: cargo install cargo-tarpaulin

      - name: Generate coverage report
        run: cargo tarpaulin --all --out Xml --output-dir coverage

      - name: Upload to Codecov
        uses: codecov/codecov-action@v3
        with:
          file: coverage/cobertura.xml
```

## 优先级

**P1（应该完成）**

- CI/CD 是现代项目的标配
- 可以快速实现基本版本

## 相关文件

- `.github/workflows/` 目录（需要创建）
- `Cargo.toml`
