#!/usr/bin/env python3
"""Run the regression suite in a temporary service without local secrets or data."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def main():
    source = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="schemist-tests-") as scratch:
        service = Path(scratch) / "ai_service_2"
        service.mkdir()
        for name in ("agents", "skills", "skills_pack", "templates", "tests", "config"):
            shutil.copytree(
                str(source / name), str(service / name),
                ignore=shutil.ignore_patterns(
                    "__pycache__", "*.pyc", ".env*", "complex_query_mappings.json",
                    "schema_aliases.json",
                ),
            )
        for name in ("app.py", "feishu_bot_api.py", "mcp_api.py"):
            shutil.copy2(str(source / name), str(service / name))
        environment = dict(os.environ)
        # All LLM, schema and execution configuration is isolated from the host.
        prefixes = ("LLM_", "SQL_", "TRINO_", "SPARK_", "SCHEMA_", "SCHEMIST_",
                    "FEISHU_", "MCP_", "PAGE_AGENT_", "PIPELINE_", "COMPLEX_QUERY_")
        for key in list(environment):
            if key.startswith(prefixes):
                environment.pop(key)
        environment.update({
            "LLM_API_URL": "http://127.0.0.1:1/v1/chat/completions",
            "LLM_API_KEY": "test-only-key",
            "SCHEMIST_WORKSPACE_DIR": scratch,
            "SCHEMA_BASE_DIRS": str(Path(scratch) / "empty-schema"),
            "SCHEMA_FEEDBACK_PATH": str(Path(scratch) / "feedback.json"),
        })
        runner = """
import logging
import socket
import sys
import unittest
from unittest.mock import patch
logging.disable(logging.CRITICAL)
with patch.object(socket.socket, 'connect', side_effect=AssertionError('Tests must not access the network')):
    suite = unittest.defaultTestLoader.discover('tests')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(not result.wasSuccessful())
"""
        return subprocess.call([sys.executable, "-c", runner], cwd=str(service), env=environment)


if __name__ == "__main__":
    sys.exit(main())
