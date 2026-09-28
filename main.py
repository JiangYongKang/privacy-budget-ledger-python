"""本地启动入口。

用法：
    uv run python main.py                 # 默认 data/ledger.db，端口 8000
    uv run python main.py --port 8011     # 自定义端口
    uv run python main.py --db /tmp/x.db  # 自定义账本文件
"""

from __future__ import annotations

import argparse

import uvicorn

from privacy_budget_ledger.api import create_app
from privacy_budget_ledger.logging_config import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="隐私预算记账服务（本地）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default="data/ledger.db", help="SQLite 账本文件路径")
    args = parser.parse_args()

    configure_logging()
    app = create_app(db_path=args.db)
    uvicorn.run(app, host=args.host, port=args.port)


app = create_app(db_path="data/ledger.db")  # 供 `uvicorn main:app` 使用


if __name__ == "__main__":
    main()
