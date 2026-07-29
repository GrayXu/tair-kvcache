# KVCacheEventSubscriber

A process co-located with an inference engine that subscribes to the engine's KV cache
events, forwards them to the kvcm service, and ties its health to the co-located
DashServing instance.

## Modules

| 模块 | 说明 |
|---|---|
| `subscriber/cli.py`、`main.py` | 进程入口与生命周期：启动 7 步、serving 监督、关停 6 步 |
| `subscriber/config.py` | 配置定义、CLI 注册、校验、派生属性 |
| `subscriber/forwarding.py`、`pipeline/` | 增量 / 快照双 pipeline 的转发链路 |
| `subscriber/engine/` | 引擎适配层（vLLM 实现 + SGLang placeholder + gRPC 控制面） |
| `subscriber/health/` | 引擎存活协调（epoch 门控）+ DashServing 状态上报 |
| `subscriber/kvcm/` | KVCM 客户端栈（注册、心跳、事件上报、服务发现） |
| `subscriber/metrics/` | 指标统一出口（主路径 telemetry + lifecycle 点事件） |
| `subscriber/proto/` | `engine_service_rpc` pb 绑定（手工维护） |

## Tech Stack

| 类别 | 技术 |
|---|---|
| 语言 | Python ≥ 3.11（asyncio 全异步） |
| 引擎事件通道 | ZeroMQ（`pyzmq`：SUB 实时流 + DEALER replay） |
| 引擎控制面 | gRPC（`grpcio` 1.75.1 + `protobuf` 3.20.3） |
| 序列化 | `msgspec`（msgpack） |
| KVCM / DashServing | HTTP JSON（`httpx`） |
| 观测 | dashlog（可选依赖，缺失时降级） |
| 工具链 | `uv`、`ruff`、`mypy --strict`、`pytest` |

## 设计文档

稳态架构文档在 `docs/architecture/`：

| 文档 | 内容 |
|---|---|
| [00-overview](docs/architecture/00-overview.md) | 模块划分、启动 / 退出时序、数据流总览、核心不变量 |
| [01-forwarding-pipeline](docs/architecture/01-forwarding-pipeline.md) | 双 pipeline 数据流、凑批、门控、丢弃语义 |
| [02-engine-adapter](docs/architecture/02-engine-adapter.md) | adapter 契约、代际语义、vLLM 实现、replay、快照信号 |
| [03-health-and-liveness](docs/architecture/03-health-and-liveness.md) | epoch、判死、HostDown、同生同死探针 |
| [04-kvcm-client](docs/architecture/04-kvcm-client.md) | 传输、服务发现、两步注册、心跳、location spec |
| [05-observability](docs/architecture/05-observability.md) | span 阶段、指标出口、日志、trace id |
| [06-configuration](docs/architecture/06-configuration.md) | 配置优先级、字段分组、环境变量、校验 |

变更设计文档在 `docs/specs/`，实施计划在 `docs/plans/`，评审与测试报告在 `docs/reviews/`，
指标目录在 `docs/metrics.json`。开发约束与流程见 [AGENTS.md](AGENTS.md)。

## Installation

```bash
uv sync --dev
uv run pre-commit install   # clone 后必须执行一次
```

## Running

Required at startup:

- `SPECTRUM_DEPLOYMENT_NAME` — unique deployment identity used to build the
  KVCM `instance_id`; startup fails if missing or blank.
- KVCM base URL — via `--kvcm-base-url` or the config file; a blank value is
  rejected by config validation.

```bash
# With CLI args
SPECTRUM_DEPLOYMENT_NAME=my-deployment uv run python -m subscriber \
  --zmq-pub-endpoint tcp://localhost:5557 \
  --kvcm-base-url spectrum://vs-example:6382

# With config file (kvcm_base_url can be set in the yaml)
SPECTRUM_DEPLOYMENT_NAME=my-deployment uv run python -m subscriber --config config.yaml
```

## Development

```bash
# Run tests
uv run pytest
uv run pytest tests/engine/vllm/test_incremental.py   # 单文件

# Lint / format
uv run ruff check subscriber/ tests/
uv run ruff format subscriber/ tests/

# Type check
uv run mypy subscriber/
```

### Quality Gates

- **pre-commit（本地强制）**：commit 时按暂存文件路由执行——`subscriber/` 或 `tests/` 的
  Python 文件触发 ruff check / ruff format --check / mypy / 全量 `pytest --cov`
  （覆盖率门禁 `fail_under = 90`）；`subscriber/proto/` 文件额外触发 pb2 import
  （protobuf 3.20.3 兼容）与 proto gRPC 测试。不得用 `--no-verify` 跳过。
- **pytest（开发中随手跑）**：commit 时 pre-commit 会强制全量测试，
  开发过程中仍应随手 `uv run pytest` 快速反馈。测试未通过的修改视为未完成。
- **AoneCI（MR 触发）**：MR opened 时触发「Python单元测试」（`pip install '.[dev]'`
  后跑 pytest）与「代码质量扫描（多语言）」两条流水线；dev 依赖必须写在
  `[project.optional-dependencies].dev`，CI 状态用 `a1 ci run get` 查询。
- **e2e（显式 opt-in）**：`tests/e2e/` 需要设置 `DSV_BINARY` 指向已构建的
  `dashservingd` 才会运行，默认全部跳过，保证各机器上默认测试行为一致。

## 基于 Agent 的开发流程

以下是本仓库的标准开发流程。**Agent 每完成一步必须停下来等确认，不要自动进入下一步。**

### 1. 明确需求与范围

**Developer:** 描述需求与边界。

**Agent:** 复述需求、列出不做什么、把不清楚的点问出来（不要猜）。

### 2. 分析代码

**Agent:** 先读 `docs/architecture/` 对应文档，再读源码，输出数据流、关键调用路径、受影响的
不变量；发现文档与代码不一致先报告。

**Developer:** 补充背景与注意事项。

### 3. 确定方案

**Agent:** 输出方案并写入 `docs/specs/YYYY-MM-DD_slug.md`（方案、受影响的不变量、测试策略、
风险）。

**Developer:** 确认方案，或指出问题让 Agent 修改。

### 4. 实施修改

**Agent:** 从最新 `master` 建分支（`feat/<description>` / `fix/<description>`），测试先行，
小步提交。

**Developer:** 检查改动是否合理。

### 5. 质量门禁

**Agent:** 跑 `uv run pytest`、`uv run ruff check subscriber/ tests/`、
`uv run mypy subscriber/`，贴出实际结论。

**Developer:** 确认全绿。

### 6. 提交与评审

**Agent:** commit → push 分支 → 创建 MR（标题与 commit message 一致，描述含背景、改动点、
验证方式、影响面与回滚方式；跨仓库改动列出对应 MR 链接）。

**Developer:** 完成代码评审。

### 7. 回顾与更新

**Agent:** 检查并更新 `docs/architecture/`、`AGENTS.md`、`README.md`、`docs/metrics.json`，
把落地方案与设计文档的差异回写到 spec。

## Protobuf Compatibility Maintenance

Production runs `protobuf==3.20.3`, and code emitted by `grpc_tools.protoc` imports
`google.protobuf.runtime_version` / `_builder`, which cannot load on that runtime.
The Python pb files in `subscriber/proto/` (`_pb2.py`, `_pb2_grpc.py`, `_pb2.pyi`) are
therefore **hand-maintained** — do **not** run `grpc_tools.protoc` and commit its output.

`subscriber/proto/engine_service_rpc.proto` remains the authoritative wire schema.
After changing the `.proto`, manually sync the pb files following the procedure in
[AGENTS.md](AGENTS.md) (section "Protobuf 兼容代码维护"): keep the
`descriptor_pb2` / `message_factory` implementation compatible with protobuf 3.20.3,
never introduce `runtime_version` or `_builder`, and only touch `_pb2_grpc.py` when the
RPC service/method/request/response types change.

The pb files are committed to the repository and are never generated at build time.
After any change, run the proto/client tests and `uv run mypy subscriber/` to confirm
they import cleanly under protobuf 3.20.3.
