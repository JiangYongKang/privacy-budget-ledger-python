# 隐私预算记账服务（privacy-budget-ledger）

本地可运行的隐私预算（Privacy Budget）记账与查询服务：以 **分析者 × 数据集**
为额度维度，支持唯一查询标识的幂等消耗、跨数据集组合原子扣减、并发不超限，
并以 SQLite 文件持久化，服务重启后已消耗额度与历史结论完整保留。

## 1. 预算口径

- 每个授权是一条 `(analyst_id, dataset_id)` 记录，字段：
  - `budget_limit`：该分析者在该数据集上的预算上限；
  - `spent`：已消耗额度，只增不减；
  - `remaining = budget_limit - spent`。
- 一次查询携带 `cost` 列表，对每个相关数据集做 `spent += cost`。
  数据库以 `CHECK (spent >= 0)` 与 `CHECK (spent <= budget_limit)` 双约束兜底，
  余额在任何时刻都不可能为负或越过上限。
- **账本单调，禁止倒退**：
  - `spent` 只随成功查询增加，服务不提供任何回退/冲正接口；
  - `budget_limit` 只允许首次授权或**上调**；任何下调请求
    （即使新上限仍高于已消耗量）一律返回 `LEDGER_ROLLBACK_REJECTED`，
    原上限与已消耗额度不变。
- 授权是显式的：分析者只能消耗自己被授权数据集上的额度。不存在的分析者、
  数据集，或未授权的 `(分析者, 数据集)` 组合，都会在进入扣减事务**之前**
  被拒绝，不产生任何扣减。

## 2. 幂等规则

- 每个查询必须带全局唯一的 `query_id`。服务端登记
  `query_id -> (请求体哈希, 扣减结果)`，登记表只追加、不修改。
- 同一 `query_id` 重复提交：
  - 请求体一致（条目按 `dataset_id` 排序后做 SHA-256，故**条目顺序不同
    也算同一请求**）：返回首次成功结论，`idempotent_replay=true`，
    **只记一次账、只给一次成功结论**，余额不变；
  - 请求体不同（金额/数据集/分析者不一致）：返回 `QUERY_ID_CONFLICT`，
    不扣减。
- 失败的提交（额度不足、越权等）**不登记** `query_id`，因此修正条件后
  可用同一标识重试。
- 登记表与扣减在同一事务写入，持久化在 SQLite 文件（默认 `data/ledger.db`）：
  服务重启后，已消耗额度、登记记录与历史结论全部保留；重启后重放重复请求
  仍然不再扣减。

## 3. 并发规则

- 存储为 SQLite（WAL 模式，`busy_timeout=5s`），每个线程一个连接；
  所有写操作使用 `BEGIN IMMEDIATE` 立即获取写锁，写事务在全库范围串行化。
- 扣减使用**条件更新**：

  ```sql
  UPDATE budgets SET spent = spent + ?
  WHERE analyst_id=? AND dataset_id=? AND spent + ? <= budget_limit
  ```

  因而并发争抢同一额度时，累计消耗永远不越上限，超限时该更新命中 0 行，
  整笔事务回滚。
- **跨数据集组合扣减是单事务**：登记查询 + 对每个数据集的扣减要么全部
  COMMIT、要么全部 ROLLBACK。任何一步失败（其中一个数据集额度不足、越权、
  实体缺失、标识冲突）都不会留下“只扣了一部分”的中间状态。
- 同一 `query_id` 被多个线程同时提交时，唯一索引 + 事务内复查保证：
  恰好一个请求真正扣减并返回首次成功结论，其余请求回放该结论，不重复扣减。

## 4. 失败原因分类

错误统一返回 `{"error": true, "error_code": ..., "message": ..., "details": ...}`，
原因码稳定可区分：

| error_code | HTTP | 含义 |
|---|---|---|
| `INVALID_PARAMETER` | 422 | 参数非法：空 ID、空条目、`cost` 非有限正数、同一请求内数据集重复等 |
| `ANALYST_NOT_FOUND` | 404 | 分析者不存在 |
| `DATASET_NOT_FOUND` | 404 | 数据集不存在 |
| `QUERY_NOT_FOUND` | 404 | 查询标识无登记记录 |
| `UNAUTHORIZED_DATASET` | 403 | 分析者未被授权访问该数据集（越权） |
| `BUDGET_INSUFFICIENT` | 409 | 扣减后将超过上限；`details` 含扣减前余额与请求金额 |
| `QUERY_ID_CONFLICT` | 409 | `query_id` 已被不同请求体占用 |
| `LEDGER_ROLLBACK_REJECTED` | 409 | 预算上限下调等任何使账本倒退的操作 |
| `ANALYST_ALREADY_EXISTS` / `DATASET_ALREADY_EXISTS` | 409 | 重复登记 |

所有失败路径都保证零扣减（成功结论只随事务 COMMIT 产生）。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/admin/analysts` | 登记分析者 `{analyst_id, name?}` |
| POST | `/admin/datasets` | 登记数据集 `{dataset_id, name?}` |
| PUT | `/admin/budgets` | 首次授权或上调 `{analyst_id, dataset_id, budget_limit}` |
| GET | `/analysts/{a}/datasets/{d}/budget` | 查询额度视图 |
| POST | `/queries` | 提交查询 `{query_id, analyst_id, items:[{dataset_id,cost}]}` |
| GET | `/queries/{query_id}` | 查询历史结论（标记 `idempotent_replay=true`） |

## 6. 本地验证

依赖 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync --group dev        # 安装运行与测试依赖

# 方式一：自动化测试（含并发争抢、重复提交、跨数据集组合、重启持久化）
uv run pytest              # 日志会打印 query_id、扣减前后余额与判定依据

# 方式二：真实 HTTP 服务端到端演示（自动启停服务，含重启验证）
bash scripts/local_demo.sh

# 方式三：手动启动
uv run python main.py --port 8000 --db data/ledger.db
# 交互式文档： http://127.0.0.1:8000/docs
```

手动调用示例：

```bash
curl -X POST localhost:8000/admin/analysts -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a1"}'
curl -X POST localhost:8000/admin/datasets -H 'Content-Type: application/json' \
  -d '{"dataset_id":"ds1"}'
curl -X PUT  localhost:8000/admin/budgets -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a1","dataset_id":"ds1","budget_limit":10}'
curl -X POST localhost:8000/queries -H 'Content-Type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","items":[{"dataset_id":"ds1","cost":3}]}'
```

## 7. 代码结构

```
src/privacy_budget_ledger/
  errors.py          # 稳定错误码与异常、错误码 -> HTTP 映射
  models.py          # 请求/响应 Pydantic 模型
  storage.py         # SQLite 持久层：WAL、IMMEDIATE 事务、条件扣减
  ledger.py          # 领域规则：校验/鉴权/幂等/组合原子扣减与判定日志
  api.py             # FastAPI 路由与异常映射
  logging_config.py  # 统一日志格式
tests/               # 34 个用例：基础口径、幂等与持久化、组合原子性、
                     #         并发争抢、HTTP 端到端
scripts/local_demo.sh
main.py              # 本地启动入口
```
