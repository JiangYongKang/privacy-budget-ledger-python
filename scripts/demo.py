#!/usr/bin/env python3
"""本地功能演示（不依赖 HTTP）：

    uv run python scripts/demo.py

覆盖：授权 -> 组合扣减 -> 重复提交幂等 -> 越权/不足/冲突的可区分拒绝
-> 跨数据集失败整体回滚 -> 重启后额度保留。
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ledger.errors import BudgetInsufficientError, LedgerError
from ledger.models import DatasetCharge
from ledger.service import build_default_service


def show(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    db = Path(tempfile.mkdtemp()) / "demo.db"
    svc = build_default_service(db)

    show("1. 注册分析者/数据集并授权：a1 -> d1=10.0, d2=5.0；a2 -> d1=2.0")
    svc.register_analyst("a1")
    svc.register_analyst("a2")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 5.0)
    svc.authorize("a2", "d1", 2.0)

    show("2. 跨数据集组合扣减 q-1：d1=4, d2=2（应成功）")
    r = svc.consume(
        query_id="q-1", analyst_id="a1",
        charges=[DatasetCharge("d1", 4.0), DatasetCharge("d2", 2.0)],
    )
    print("status:", "idempotent_replay" if r.replay else "accepted",
          "total:", r.total_charged)

    show("3. 重复提交同一 q-1（幂等：只给同一结论、不重复扣）")
    r2 = svc.consume(
        query_id="q-1", analyst_id="a1",
        charges=[DatasetCharge("d1", 4.0), DatasetCharge("d2", 2.0)],
    )
    print("status:", "idempotent_replay" if r2.replay else "accepted",
          "d1 余额:", svc.get_balance("a1", "d1").balance)

    show("4. 各类失败原因可区分")
    for label, kwargs in [
        ("越权 a2 访问 d2", dict(query_id="x1", analyst_id="a2",
                                 charges=[DatasetCharge("d2", 1.0)])),
        ("额度不足 d1=99", dict(query_id="x2", analyst_id="a1",
                                charges=[DatasetCharge("d1", 99.0)])),
        ("标识冲突 q-1 改内容", dict(query_id="q-1", analyst_id="a1",
                                    charges=[DatasetCharge("d1", 1.0)])),
        ("参数非法 cost<=0", dict(query_id="x3", analyst_id="a1",
                                  charges=[DatasetCharge("d1", 0.0)])),
    ]:
        try:
            svc.consume(**kwargs)
        except LedgerError as e:
            print(f"{label:24s} -> code={e.code} ({e.http_status})")

    show("5. 组合扣减任一步失败则整体不生效：d1=1(足), d2=99(不足)")
    try:
        svc.consume(query_id="q-2", analyst_id="a1",
                    charges=[DatasetCharge("d1", 1.0), DatasetCharge("d2", 99.0)])
    except BudgetInsufficientError as e:
        print("被拒绝:", e.code)
    print("d1 消耗仍为:", svc.get_balance("a1", "d1").consumed,
          " d2 消耗仍为:", svc.get_balance("a1", "d2").consumed)

    show("6. 重启服务（重新打开同一文件库），额度与查询记录保留")
    svc._store.close()
    svc = build_default_service(db)
    print("d1:", svc.get_balance("a1", "d1"))
    print("d2:", svc.get_balance("a1", "d2"))
    print("q-1 记录:", svc.get_query_record("q-1").total_charged)
    svc._store.close()


if __name__ == "__main__":
    main()
