#!/usr/bin/env bash
# 本地端到端演示：启动 -> 授权 -> 幂等 -> 越权/不足/冲突区分 -> 重启保留。
# 用法: bash scripts/local_demo.sh
set -euo pipefail

PORT="${PORT:-8011}"
BASE="http://127.0.0.1:${PORT}"
DB="data/demo.db"
rm -f "$DB" "$DB-wal" "$DB-shm"

uv run python main.py --port "$PORT" --db "$DB" >/tmp/pbl_demo.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

echo '== 等待服务就绪 =='
for _ in $(seq 1 50); do
  curl -sf "$BASE/health" >/dev/null && break
  sleep 0.2
done

jget() { python3 -c 'import sys,json;d=json.load(sys.stdin);print('"$1"')'; }

echo; echo '== 1. 登记分析者/数据集并授权 (a1: ds1=10, ds2=10; a2: ds1=10) =='
curl -sS -X POST "$BASE/admin/analysts" -H 'Content-Type: application/json' -d '{"analyst_id":"a1"}' >/dev/null
curl -sS -X POST "$BASE/admin/analysts" -H 'Content-Type: application/json' -d '{"analyst_id":"a2"}' >/dev/null
curl -sS -X POST "$BASE/admin/datasets" -H 'Content-Type: application/json' -d '{"dataset_id":"ds1"}' >/dev/null
curl -sS -X POST "$BASE/admin/datasets" -H 'Content-Type: application/json' -d '{"dataset_id":"ds2"}' >/dev/null
curl -sS -X PUT  "$BASE/admin/budgets"  -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a1","dataset_id":"ds1","budget_limit":10}' >/dev/null
curl -sS -X PUT  "$BASE/admin/budgets"  -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a1","dataset_id":"ds2","budget_limit":10}' >/dev/null
curl -sS -X PUT  "$BASE/admin/budgets"  -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a2","dataset_id":"ds1","budget_limit":10}' >/dev/null
echo done

echo; echo '== 2. 组合扣减 (q-1: ds1 花 3, ds2 花 2) =='
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","items":[{"dataset_id":"ds1","cost":3},{"dataset_id":"ds2","cost":2}]}' \
  | python3 -m json.tool

echo; echo '== 3. 重复提交 q-1（应 idempotent_replay=true，余额不变）=='
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","items":[{"dataset_id":"ds2","cost":2},{"dataset_id":"ds1","cost":3}]}' \
  | jget 'd["idempotent_replay"], d["entries"][0]["spent_after"]'

echo; echo '== 4. 失败原因可区分 =='
echo -n '不存在分析者: '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-x","analyst_id":"ghost","items":[{"dataset_id":"ds1","cost":1}]}' \
  | jget 'd["error_code"]'
echo -n '越权访问:     '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-x","analyst_id":"a2","items":[{"dataset_id":"ds2","cost":1}]}' \
  | jget 'd["error_code"]'
echo -n '额度不足:     '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-big","analyst_id":"a1","items":[{"dataset_id":"ds1","cost":99}]}' \
  | jget 'd["error_code"]'
echo -n '标识冲突:     '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","items":[{"dataset_id":"ds1","cost":9}]}' \
  | jget 'd["error_code"]'
echo -n '参数非法:     '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-x","analyst_id":"a1","items":[]}' \
  | jget 'd["error_code"]'
echo -n '额度回退:     '
curl -sS -X PUT "$BASE/admin/budgets" -H 'Content-Type: application/json' \
  -d '{"analyst_id":"a1","dataset_id":"ds1","budget_limit":5}' \
  | jget 'd["error_code"]'

echo; echo '== 5. 重启服务，余额与幂等结论必须保留 =='
kill $SERVER_PID; wait $SERVER_PID 2>/dev/null || true
uv run python main.py --port "$PORT" --db "$DB" >/tmp/pbl_demo.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 50); do curl -sf "$BASE/health" >/dev/null && break; sleep 0.2; done
echo -n '重启后 ds1 已消耗(应为3): '
curl -sS "$BASE/analysts/a1/datasets/ds1/budget" | jget 'd["spent"]'
echo -n '重启后重放 q-1:           '
curl -sS -X POST "$BASE/queries" -H 'Content-Type: application/json' \
  -d '{"query_id":"q-1","analyst_id":"a1","items":[{"dataset_id":"ds1","cost":3},{"dataset_id":"ds2","cost":2}]}' \
  | jget 'd["idempotent_replay"]'
echo -n '重启后 ds1 仍为 3:        '
curl -sS "$BASE/analysts/a1/datasets/ds1/budget" | jget 'd["spent"]'

echo; echo '演示完成，服务日志: /tmp/pbl_demo.log'
