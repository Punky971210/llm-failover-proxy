from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

LOG_DIR = Path(os.environ.get("PROXY_LOG_DIR", "C:/Users/Administrator/.jiuwenswarm/provider-switch-log"))


def setup_logger(name: str = "llm-failover") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    # ── stdout handler ────────────────────────────────────────────────
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    logger.addHandler(stdout_handler)

    # ── file handler (rotating via RotatingFileHandler) ────────────────
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        from logging.handlers import RotatingFileHandler

        file_handler = RotatingFileHandler(
            LOG_DIR / "proxy.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)
    except Exception:
        logger.warning("无法创建日志文件，仅使用 stdout")

    return logger
