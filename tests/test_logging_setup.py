import io
import logging
import os
import subprocess
import sys
import pytest

from logging_setup import configure_logging


@pytest.fixture(autouse=True)
def no_default_file(monkeypatch):
    monkeypatch.setenv("LOG_FILE", "")


def test_console_logging_works_with_existing_memory_handler():
    root = logging.RootLogger(logging.INFO)
    captured = []

    class Memory(logging.Handler):
        def emit(self, record):
            captured.append(record.getMessage())

    memory = Memory()
    root.addHandler(memory)
    stream = io.StringIO()
    handler = configure_logging(root, stream)
    root.info("Transfer abc feed=100 Telegram=200")
    assert "Transfer abc feed=100 Telegram=200" in stream.getvalue()
    assert captured == ["Transfer abc feed=100 Telegram=200"]
    assert memory in root.handlers
    assert configure_logging(root, stream) is handler
    assert len(root.handlers) == 2


def test_debug_does_not_enable_wire_logging(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    root = logging.RootLogger(logging.WARNING)
    configure_logging(root, io.StringIO())
    try:
        assert root.level == logging.INFO
        assert logging.getLogger("streaming").level == logging.DEBUG
    finally:
        monkeypatch.setenv("LOG_LEVEL", "INFO")
        configure_logging(root, io.StringIO())


def test_main_import_emits_console_and_keeps_bot_logs(tmp_path):
    # Reproduce the real import order in a fresh process, without starting the
    # bot, opening its session database or making a network request.
    source = ("import main, bot; main.logger.info('CONSOLE_LOGGING_SMOKE'); "
              "assert any('CONSOLE_LOGGING_SMOKE' in row for row in bot.memory_log_handler.get_logs())")
    result = subprocess.run([sys.executable, "-c", source], capture_output=True, timeout=30,
                            env={**os.environ, "LOG_LEVEL": "INFO", "LOG_FILE": str(tmp_path / "bot.log")})
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert b"CONSOLE_LOGGING_SMOKE" in result.stdout
    assert "CONSOLE_LOGGING_SMOKE" in (tmp_path / "bot.log").read_text(encoding="utf-8")


def test_rotating_file_logging_is_bounded_and_not_duplicated(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_FILE", str(tmp_path / "bot.log"))
    root = logging.RootLogger(logging.INFO)
    configure_logging(root, io.StringIO())
    configure_logging(root, io.StringIO())
    files = [h for h in root.handlers if getattr(h, "_bot_file", False)]
    assert len(files) == 1
    handler = files[0]
    try:
        assert handler.maxBytes == 2 * 1024 * 1024 and handler.backupCount == 2
        root.info("persistent transfer diagnostics")
        assert "persistent transfer diagnostics" in (tmp_path / "bot.log").read_text(encoding="utf-8")
    finally:
        handler.close()
