"""本地启动入口：python main.py 后访问 http://127.0.0.1:8000"""

from __future__ import annotations

import uvicorn

from ledger.api import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
