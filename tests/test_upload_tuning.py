from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyrogram
import pytest

import config
import upload_tuning
from upload_tuning import AdaptiveUploadWindow


def samples(control, rate, **kwargs):
    return [control.observe(rate, backlog=True, saturated=True, **kwargs) for _ in range(2)]


def test_trials_increase_connections_then_pipeline_and_stop_at_ceiling():
    control = AdaptiveUploadWindow(8, 4, 16, 8)
    assert control.capacity == 32
    assert samples(control, 100) == [False, True]
    assert (control.workers, control.depth) == (16, 4)
    samples(control, 170)
    assert control.capacity == 128
    samples(control, 250)
    assert control.capacity == 128 and control.stopped


def test_plateau_restores_previous_setting():
    control = AdaptiveUploadWindow(8, 4, 16, 8)
    samples(control, 100)
    samples(control, 102)
    assert control.capacity == 32 and control.stopped
    samples(control, 1000)
    assert control.capacity == 32


def test_slower_trial_restores_previous_setting():
    control = AdaptiveUploadWindow(4, 4, 16, 8)
    samples(control, 100)
    samples(control, 60)
    assert control.capacity == 16 and control.stopped


@pytest.mark.parametrize("backlog,saturated", [(False, True), (True, False), (False, False)])
def test_source_limited_or_idle_transfers_do_not_expand(backlog, saturated):
    control = AdaptiveUploadWindow(4, 4, 16, 8)
    for _ in range(10):
        control.observe(100, backlog=backlog, saturated=saturated)
    assert control.capacity == 16


def test_throttling_backs_off_even_before_first_growth():
    control = AdaptiveUploadWindow(8, 4, 16, 8)
    control.observe(100, backlog=True, saturated=True, congested=True)
    assert (control.workers, control.depth) == (4, 4)
    assert control.stopped
    control.observe(100, backlog=True, saturated=True, congested=True)
    assert control.capacity == 8


def test_throttled_trial_reverts_and_disables_probing():
    control = AdaptiveUploadWindow(8, 4, 16, 8)
    samples(control, 100)
    control.observe(200, backlog=True, saturated=True, congested=True)
    assert control.capacity == 32 and control.stopped


def test_fixed_mode_and_duplicate_limits():
    control = AdaptiveUploadWindow(4, 4, 16, 8, enabled=False)
    samples(control, 100, congested=True)
    assert control.capacity == 16
    limited = AdaptiveUploadWindow(4, 4, 4, 4)
    samples(limited, 100)
    assert limited.capacity == 16 and limited.stopped


def test_crypto_pool_is_configured_before_clients_start_and_is_idempotent(monkeypatch):
    monkeypatch.setattr(config, "CRYPTO_WORKERS", 4)
    previous = SimpleNamespace(shutdown=Mock())
    replacement = SimpleNamespace()
    with patch.object(pyrogram, "crypto_executor", previous), \
         patch.object(upload_tuning, "ThreadPoolExecutor", return_value=replacement) as pool:
        assert upload_tuning.configure_crypto_workers() == 4
        assert pyrogram.crypto_executor is replacement
        assert upload_tuning.configure_crypto_workers() == 4
        pool.assert_called_once_with(4, thread_name_prefix="TelegramCrypto")
    previous.shutdown.assert_called_once_with(wait=False)
