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


def test_handshake_and_partial_window_samples_cannot_reject_a_fast_trial():
    control = AdaptiveUploadWindow(4, 4, 8, 8)
    samples(control, 3000)
    assert (control.workers, control.depth) == (8, 4)
    # Reproduce the supplied log: four sequential handshakes occupy most of
    # the first trial interval. It is not a measurement of eight ready sockets.
    samples(control, 1200, settled=False)
    assert control.level == 1 and not control.stopped
    assert control.samples == [] and control.baseline == 3000
    samples(control, 5500)
    assert (control.workers, control.depth) == (8, 8)


def test_errors_still_back_off_during_connection_warmup():
    control = AdaptiveUploadWindow(4, 4, 8, 8)
    samples(control, 3000)
    control.observe(1200, True, True, congested=True, settled=False)
    assert (control.workers, control.depth) == (4, 4)
    assert control.stopped and control.reason == "retry/throttle backoff"


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


def test_backoff_can_recover_original_capacity_in_same_video():
    control = AdaptiveUploadWindow(8, 4, 16, 8, recovery_seconds=30)
    control.observe(100, True, True, congested=True, now=0)
    assert control.workers == 4
    samples(control, 100, now=29)
    assert control.workers == 4
    samples(control, 100, now=31)
    assert control.workers == 8
    samples(control, 200, now=32)
    assert control.workers == 16


def test_plateau_is_retested_after_cooldown():
    control = AdaptiveUploadWindow(8, 4, 16, 8, recovery_seconds=30)
    samples(control, 100, now=0)
    samples(control, 100, now=1)
    assert control.workers == 8 and control.stopped
    samples(control, 100, now=32)
    assert control.workers == 16


def test_repeated_throttles_have_a_floor_and_do_not_destroy_recovery_levels():
    control = AdaptiveUploadWindow(8, 4, 16, 8, recovery_seconds=30)
    for second in range(10):
        control.observe(100, True, True, congested=True, now=second)
    assert control.workers == 2
    assert (8, 4) in control.levels and (16, 8) in control.levels


def test_profile_reuse_is_scoped_to_client_dc_limits_and_ttl():
    from upload_tuning import previous_upload_setting, remember_upload_setting
    client = SimpleNamespace()
    control = AdaptiveUploadWindow(8, 4, 16, 8)
    samples(control, 100)
    samples(control, 200)
    remember_upload_setting(client, 2, False, control, 32 * 1024 * 1024)
    assert previous_upload_setting(client, 2, False, 8, 4, 16, 8) == (16, 4)
    assert previous_upload_setting(client, 3, False, 8, 4, 16, 8) is None
    assert previous_upload_setting(client, 2, True, 8, 4, 16, 8) is None
    assert previous_upload_setting(client, 2, False, 8, 4, 8, 8) is None
    with patch.object(upload_tuning.time, "monotonic", return_value=client._bot_upload_profile.saved_at + 1801):
        assert previous_upload_setting(client, 2, False, 8, 4, 16, 8) is None
    remember_upload_setting(client, 2, False, control, 32 * 1024 * 1024, throttled=True)
    assert client._bot_upload_profile is None


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
