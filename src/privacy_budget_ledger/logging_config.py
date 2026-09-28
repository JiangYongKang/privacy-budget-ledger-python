"""日志配置：测试与服务共用同一套格式，便于打印判定依据。"""

from __future__ import annotations

import logging

FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    """配置根 logger（幂等，重复调用不会叠加 handler）。"""
    global _configured
    root = logging.getLogger()
    if _configured:
        root.setLevel(level)
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FMT))
    root.addHandler(handler)
    root.setLevel(level)
    _configured = True
