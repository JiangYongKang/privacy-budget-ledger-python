# 隐私预算记账本（privacy-budget-ledger）

一套可在本地直接运行的隐私预算（privacy budget）记账与查询服务，围绕
**分析者 × 数据集** 两个维度管理额度，支持以唯一查询标识做幂等消耗、
一次查询跨多个数据集的组合扣减、并发争抢下的强一致、服务重启后的状态保留，
以及**“先预占额度、任务跑完再按实际用量结算”**（reservation）的两阶段记账。

- 语言/运行时：Python 3.14 + FastAPI + SQLite（标准库 `sqlite3`，无额外服务依赖）
- 存储模型：**只追加事件账本**（append-only event log），没有可被覆盖的"当前余额"

---

## 1. 目录结构

```
ledger/
  errors.py    # 可区分的领域错误码（失败原因分类）
  models.py    # 领域模型：DatasetCharge / ReservationItem / 各 Result / BalanceView
  store.py     # SQLite 只追加事件账本 + 预占表 + 单写事务 + 启动完整性校验
  service.py   # 应用服务：参数校验、鉴权顺序、幂等判定、结构化日志
  api.py       # FastAPI HTTP 接口与错误响应映射
main.py        # 本地启动入口
scripts/demo.py# 不依赖 HTTP 的功能演示（含预占/结算/归还/过期）
tests/         # 单测：错误分类 / 幂等 / 多数据集 / 并发 / 持久化 / 预占 / HTTP
```

---

## 2. 预算口径

- 额度按 `(analyst_id, dataset_id)` 二元组授权，字段为 `limit`（上限）。
- `consumed`（已消耗）**只由 `events` 表中的成功扣减事件（直接扣减 + 预占结算）
  求和得到**，账本中不存在可就地修改的余额字段。
- `reserved`（预占中）= 状态为 `held` 且**未过有效期**的预占金额之和。
- 统一口径（任何时刻都必须成立）：

  ```
  consumed（已扣减） + reserved（未过期预占） ≤ limit
  available（还能动用） = limit − consumed − reserved
  ```

  预占与直接扣减是**同一本账**：预占占住的额度别人抢不走，但也不会被重复计算；
  结算时预占"转"成已扣减（差额释放），而不是额外再扣一次。
- 余额接口额外返回 `reserved` 与 `available` 字段。
- 一次请求可携带多个扣减/预占项，同一次请求内同一数据集只允许出现一次（需自行合并）。
- 授权上限允许**不变或上调**；**下调上限被视为账本倒退，直接拒绝**
  （`ledger_rollback_detected`）。
- 每个扣减/预占金额必须是正的有限数（>0，不接受 NaN/Inf/0/负数）；
  结算的实际用量允许为 0（表示该数据集没用到、预占全额退回）。

---

## 3. 幂等规则

- 每个请求必须带全局唯一的 `query_id`；以
  `UNIQUE(query_id, dataset_id)` 约束保证同一查询的每个数据集只会有一条事件。
- 服务端处理顺序（全部在同一个写事务内）：
  1. 分析者存在 → 2. 每个数据集存在 → 3. 每个数据集已被该分析者授权
  → 4. 查 `query_id` 是否已存在成功事件
  → 5. 事务内复核每个数据集余额是否充足
  → 6. 一次性写入全部事件并提交。
- **重复提交**：`(query_id, 数据集集合, 每项 cost, 所属分析者)` 完全一致时，
  不再扣减，直接重放首次的成功结论（响应 `status=idempotent_replay`，
  返回的每项"扣减前/后余额"与首次完全一致）。
- **标识冲突**：`query_id` 已存在但扣减内容或分析者不一致 →
  `409 query_conflict`，不产生任何扣减。
- 失败的请求不写入事件，因此**不会占用 `query_id`**；同一 `query_id`
  用合法参数重试可以成功。

## 4. 预占（先预占、后结算/归还）

适用于"任务开跑前不知道确切用量，先把额度占住"的场景。一笔预占有自己的
全局唯一标识 `reservation_id` 和状态机：

```
held（占用中） ──settle──▶ settled（已按实际用量结算）
     │
     ├────release──▶ released（任务取消，整笔归还）
     │
     └───超过有效期──▶ expired（系统自动释放，无需也不能再结算/归还）
```

### 4.1 三个操作

- **预占 `POST /reservations`**：携带一个或多个 `{dataset_id, amount}`，
  在**同一个写事务**内全部占上或整体不生效；返回 `expires_at`（到期时刻）。
  占住期间这部分额度计入 `reserved`，其他请求（含直接扣减）抢不走。
- **结算 `POST /reservations/{id}/settle`**：任务跑完按实际用量 `actual` 结账。
  - 每个预占过的数据集都要给一个实际用量（没用就给 `0`，表示全额退回）；
  - `实际 ≤ 预占`：实际部分由"占用"转为"已扣减"，多占的差额立即退回 `available`；
  - 不允许实际用量超过该数据集的预占额（`invalid_parameter`，不动任何额度）。
- **归还 `POST /reservations/{id}/release`**：任务不做了，整笔释放占用。

### 4.2 幂等：重复提交只生效一次、结论一致

预占/结算/归还都以 `reservation_id` 为幂等键：

- 预占重复提交且 `(分析者, 数据集集合, 每项 amount)` 完全一致 →
  重放首次结论（`idempotent_replay`，`expires_at` 以首次为准，不因后来的
  `ttl_seconds` 参数改变）；内容或分析者不一致 → `409 reservation_conflict`。
- 结算重复提交且实际用量一致 → 重放首次结算结论，**不会二次扣账**；
  实际用量与首次不一致 → `409 reservation_conflict`。
- 归还重复提交 → 重放首次结论，**不会重复退款**。
- 失败的请求不落库、不占用 `reservation_id`。

### 4.3 有效期与自动释放

- 每笔预占带 `expires_at = 创建时刻 + ttl_seconds`；到期未结算/归还的，
  状态自动变 `expired`，占用额度自动退回。
- TTL 可配置（优先级：请求里的 `ttl_seconds` > 服务默认值）：
  - 服务默认值：构造 `BudgetService(default_ttl_seconds=...)`，或环境变量
    `BUDGET_LEDGER_RESERVATION_TTL`（秒，默认 3600）；
  - 本地想快速验证：预占时直接带一个很小的 `ttl_seconds`（如 1 秒），
    或给服务/存储注入可控时钟 `clock`（测试即用此法）。
- 过期判定是**惰性 + 主动**双保险：每次写事务开始时先清理到期预占，
  也可显式调用 `sweep_expired()` / 重启 `open()` 时清理。
- **过期后再结算被明确拒绝**：`410 reservation_expired`，而不是照常扣账；
  过期后再归还同样返回 `reservation_expired`（系统已自动释放，避免重复退款）。
- 有效期随预占单一起落盘：**服务重启后，仍在有效期内的预占继续有效**，
  占用状态和额度都不丢；停机期间到期的，重启打开库时先置 `expired` 再做
  完整性校验，额度不会凭空增多或减少。

### 4.4 与直接扣减 / 幂等 / 并发规则的共存

- **一本账**：直接扣减与预占共用第 2 节的统一口径
  `consumed + reserved(未过期) ≤ limit`；两者并发混跑时在事务内复核同一条线，
  任何时刻都不越上限。
- **同一套事务机制**：组合预占与组合扣减一样，单写锁 + `BEGIN IMMEDIATE` +
  单事务批量落库，任一步失败整体 `ROLLBACK`。
- **同一套实体/授权/参数判定顺序**：没授权、对象不存在、参数不合法、
  同一标识内容不一致等失败原因仍然各自可区分，失败时不动任何额度。
- 结算产生的扣减事件使用 `settle:<reservation_id>` 作为 `query_id` 命名空间，
  与直接扣减的 `query_id` 互不干扰，同样进入只追加事件账本与重启回放。

## 5. 并发与事务规则

- 进程内所有数据库访问由一把可重入锁串行化，写事务使用
  `BEGIN IMMEDIATE`；多数据集组合扣减/预占在**同一个事务**内完成，
  任一步失败立即 `ROLLBACK`：要么全部成功，要么整体不生效，
  不可能留下"只扣/只占了一部分"的中间状态。
- 充足性在事务内、落库前按统一口径 `consumed + 未过期reserved + 本次 ≤ limit`
  复核，因此直接扣减与预占并发争抢同一份额度时也不会越过 `limit`
  （详见 `tests/test_concurrency.py` 与 `tests/test_reservation_concurrency.py`）。
- SQLite 数据库文件默认开启 WAL；默认库路径 `data/budget_ledger.db`，
  可用环境变量 `BUDGET_LEDGER_DB` 覆盖。
- **重启持久化与防倒退**：重新打开库时先释放停机期间到期的预占，再按 `seq`
  顺序回放全部事件并校验：
  - 事件序号必须从 1 连续无缺口（删除事件 → 检出缺口）；
  - 每个事件必须能找到对应授权；
  - 回放累计消耗不得超过授权上限（篡改上限 → 检出越限）；
  - 未过期预占 + 已扣减之和不得越过授权上限，结算额不得超过预占额。

  任一校验失败即抛 `ledger_rollback_detected`，拒绝在可疑账本上继续服务，
  而不是从某个中间状态继续记账。

---

## 6. 失败原因分类（可区分错误码）

所有错误响应统一为：

```json
{"error": {"code": "<machine_readable>", "message": "…", "details": { … }}}
```

| code | HTTP | 触发条件 | 是否产生扣减 |
|---|---|---|---|
| `analyst_not_found` | 404 | 分析者不存在 | 否 |
| `dataset_not_found` | 404 | 数据集不存在（多数据集中任一不存在即整体失败） | 否 |
| `unauthorized_dataset` | 403 | 分析者未被授权访问该数据集（越权） | 否 |
| `budget_insufficient` | 409 | 任一数据集可用额度不足（`consumed+reserved+本次>limit`，整体回滚） | 否 |
| `query_conflict` | 409 | `query_id` 已存在但扣减内容/分析者不一致 | 否 |
| `reservation_not_found` | 404 | 结算/归还/查询了不存在（或不属于该分析者）的预占 | 否 |
| `reservation_conflict` | 409 | 预占标识已存在但预占内容/分析者不一致，或重复结算的实际用量不一致 | 否 |
| `reservation_state_invalid` | 409 | 状态机不允许：对已归还的预占结算、对已结算的预占归还 | 否 |
| `reservation_expired` | 410 | 预占已过有效期被自动释放，再结算/归还被明确拒绝 | 否（系统已释放占用） |
| `invalid_parameter` | 422 | 空标识、空列表、金额非正/非有限数、同请求内数据集重复、结算缺项或实际超预占、body 校验失败 | 否 |
| `ledger_rollback_detected` | 500 | 事件序列缺口/回放越限/预占+扣减越限/试图下调上限等账本倒退 | 服务拒绝继续记账 |
| `query_not_found` | 404 | 查询不存在的 `query_id` 明细（`GET /queries/{id}`） | — |

---

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/analysts` | `{"id": "a1"}` 注册分析者 |
| POST | `/datasets` | `{"id": "d1"}` 注册数据集 |
| PUT | `/analysts/{analyst_id}/datasets/{dataset_id}/grant` | `{"limit": 10.0}` 授权/上调 |
| GET | `/analysts/{analyst_id}/datasets/{dataset_id}/balance` | 查询单个额度（含 `reserved`/`available`） |
| GET | `/analysts/{analyst_id}/balances` | 查询该分析者所有授权额度 |
| POST | `/consume` | 幂等直接扣减（支持多数据集） |
| GET | `/queries/{query_id}` | 按查询标识回放成功结论 |
| POST | `/reservations` | 预占额度（支持多数据集、可带 `ttl_seconds`） |
| POST | `/reservations/{reservation_id}/settle` | 按实际用量结算，多占退回 |
| POST | `/reservations/{reservation_id}/release` | 整笔归还预占 |
| GET | `/reservations/{reservation_id}` | 查询预占状态与明细 |
| GET | `/health` | 健康检查 |

`POST /consume` 请求示例：

```json
{
  "query_id": "q-20260928-001",
  "analyst_id": "a1",
  "charges": [
    {"dataset_id": "d1", "cost": 4.0},
    {"dataset_id": "d2", "cost": 2.0}
  ]
}
```

成功响应（首次 `accepted`；重复提交为 `idempotent_replay`，内容一致）：

```json
{
  "query_id": "q-20260928-001",
  "analyst_id": "a1",
  "status": "accepted",
  "total_charged": 6.0,
  "items": [
    {"dataset_id": "d1", "charged": 4.0, "balance_before": 10.0, "balance_after": 6.0},
    {"dataset_id": "d2", "charged": 2.0, "balance_before": 5.0,  "balance_after": 3.0}
  ]
}
```

预占 → 结算 / 归还示例：

```bash
# 预占（可跨多数据集；ttl_seconds 可选，用于把有效期调短快速验证）
curl -s -X POST localhost:8000/reservations -H 'content-type: application/json' -d '{
  "reservation_id": "rv-001", "analyst_id": "a1",
  "items": [{"dataset_id": "d1", "amount": 6}, {"dataset_id": "d2", "amount": 2}],
  "ttl_seconds": 60
}'
# -> status=accepted, 并返回 expires_at；占用期间 balance 接口的 available 会下降

# 跑完按实际用量结算（每个预占数据集都要给数，没用给 0；多占自动退回）
curl -s -X POST localhost:8000/reservations/rv-001/settle \
  -H 'content-type: application/json' -d '{
    "analyst_id": "a1",
    "actual": [{"dataset_id": "d1", "amount": 4}, {"dataset_id": "d2", "amount": 0}]
  }'
# -> status=settled, total_settled=4.0, total_refunded=4.0；再发同样请求为 idempotent_replay

# 任务不做了就整笔归还（与结算二选一）
curl -s -X POST localhost:8000/reservations/rv-001/release \
  -H 'content-type: application/json' -d '{"analyst_id": "a1"}'

# 查状态：held / settled / released / expired
curl -s localhost:8000/reservations/rv-001
```

---

## 8. 本地验证方法

前置：已安装 [uv](https://docs.astral.sh/uv/)。

```bash
# 1) 安装依赖（含 pytest / httpx）
uv sync

# 2) 跑全部单测（日志会实时打印 查询标识 / 扣减前后余额 / 判定依据）
uv run pytest -q

# 3) 不依赖 HTTP 的功能演示：授权、直接扣减、预占/结算/归还、短TTL过期、
#    幂等、越权、不足、冲突、组合回滚、重启保留
uv run python scripts/demo.py

# 4) 启动 HTTP 服务（默认 http://127.0.0.1:8000，库文件 data/budget_ledger.db）
uv run python main.py
#   交互文档：http://127.0.0.1:8000/docs

# 想把预占默认有效期调短做联调（单位：秒，默认 3600）
BUDGET_LEDGER_RESERVATION_TTL=10 uv run python main.py
```

curl 快速体验（含预占；为方便快速验证用 `ttl_seconds=2`）：

```bash
curl -s -X POST localhost:8000/analysts -H 'content-type: application/json' -d '{"id":"a1"}'
curl -s -X POST localhost:8000/datasets -H 'content-type: application/json' -d '{"id":"d1"}'
curl -s -X PUT  localhost:8000/analysts/a1/datasets/d1/grant -H 'content-type: application/json' -d '{"limit":10}'
curl -s -X POST localhost:8000/consume -H 'content-type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","charges":[{"dataset_id":"d1","cost":4}]}'
# 再发一次同样的请求，status 变为 idempotent_replay，余额不变
curl -s localhost:8000/analysts/a1/datasets/d1/balance
# 预占4，可用变成2；等2秒后结算 -> 410 reservation_expired，占用自动释放
curl -s -X POST localhost:8000/reservations -H 'content-type: application/json' \
  -d '{"reservation_id":"rv-x","analyst_id":"a1","items":[{"dataset_id":"d1","amount":4}],"ttl_seconds":2}'
curl -s localhost:8000/analysts/a1/datasets/d1/balance  # reserved=4, available=2
sleep 2
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/reservations/rv-x/settle \
  -H 'content-type: application/json' -d '{"analyst_id":"a1","actual":[{"dataset_id":"d1","amount":1}]}'
```

### 测试覆盖说明

- `tests/test_service_errors.py`：5 类业务失败原因可区分、失败零扣减、拒绝下调上限；
- `tests/test_idempotency.py`：同 `query_id` 多次提交只扣一次且结论一致、可追溯；
- `tests/test_multi_dataset.py`：跨数据集全成功/整体回滚、混合查询后口径稳定；
- `tests/test_concurrency.py`：40 线程争抢不越限、25 线程并发同标识只扣一次、
  8 线程组合扣减无部分状态、60 线程混合压力结果稳定；
- `tests/test_persistence.py`：重启后额度与幂等记录保留、删事件/篡改上限被判定为倒退；
- `tests/test_api.py`：HTTP 端到端错误码、幂等响应、统一错误信封；
- `tests/test_reservations.py`：预占→结算（多退少补/零用量全退）、预占→归还、
  三类操作各自幂等、内容不一致冲突、状态机非法转移、各类实体/参数失败可区分、
  结算超预占拒绝且零变动、统一口径边界；
- `tests/test_reservation_multi_expiry.py`：跨数据集组合预占全有/全无、
  组合结算按数据集各自退回、到期自动释放且过期结算 410、惰性释放不凭空造额度、
  过期归还拒绝（防重复退款）、TTL 可配置；
- `tests/test_reservation_concurrency.py`：40 线程预占/直接扣减混跑不越限、
  同标识并发预占/并发结算只生效一次；
- `tests/test_reservation_persistence.py`：在持预占重启后仍占用且可结算、
  停机期间到期重启自动释放、顶满上限的在持预占重启完整性校验放行；
- `tests/test_reservation_api.py`：预占/结算/归还/查询 HTTP 端到端、
  过期 410、各类 404/409/422 状态码、统一错误信封。

测试日志统一由 `privacy_budget_ledger` logger 输出，关键字段：
`query_id`、`reservation_id`、`analyst_id`、`dataset_id`、`本次扣减/预占/结算`、
`退回`、`占用后/结算后可用`、`到期`、`判定依据`。
