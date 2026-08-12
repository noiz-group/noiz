from types import SimpleNamespace

from noiz.api.qc import (
    _calculate_qctwo_results_batch_by_id_wrapper,
    _prepare_qctwo_parallel_task_inputs,
)


class _DummyAppContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyApp:
    def app_context(self):
        return _DummyAppContext()


def test_prepare_qctwo_parallel_task_inputs_chunks_ids():
    inputs = _prepare_qctwo_parallel_task_inputs(
        ccf_id_batch=tuple(range(1, 8)),
        qctwo_config_id=42,
        per_task_chunk_size=3,
    )

    assert inputs == [
        {"qctwo_config_id": 42, "crosscorrelation_cartesian_ids": (1, 2, 3)},
        {"qctwo_config_id": 42, "crosscorrelation_cartesian_ids": (4, 5, 6)},
        {"qctwo_config_id": 42, "crosscorrelation_cartesian_ids": (7,)},
    ]


def test_calculate_qctwo_results_batch_by_id_wrapper_loads_worker_side(monkeypatch):
    qctwo_config = SimpleNamespace(
        id=99,
        time_periods_rejected=(),
        componentpair_ids_rejected_times=(),
        null_value=None,
    )
    ccfs = [SimpleNamespace(id=5), SimpleNamespace(id=9)]
    called = {"config_ids": [], "fetched_ids": [], "calculated": [], "removed": 0}

    monkeypatch.setattr("noiz.api.qc._get_qctwo_worker_app", lambda: _DummyApp())
    monkeypatch.setattr(
        "noiz.api.qc.fetch_qctwo_config_single",
        lambda id: called["config_ids"].append(id) or qctwo_config,
    )
    monkeypatch.setattr(
        "noiz.api.qc._fetch_crosscorrelation_cartesian_for_qctwo_ids",
        lambda crosscorrelation_cartesian_ids: called["fetched_ids"].append(tuple(crosscorrelation_cartesian_ids))
        or ccfs,
    )

    def _fake_calculate(inputs):
        called["calculated"].append((inputs["crosscorrelation_cartesian"].id, inputs["qctwo_config"].id))
        return (f"result-{inputs['crosscorrelation_cartesian'].id}",)

    monkeypatch.setattr("noiz.api.qc.calculate_qctwo_results_wrapper", _fake_calculate)
    monkeypatch.setattr(
        "noiz.api.qc.db.session.remove",
        lambda: called.__setitem__("removed", called["removed"] + 1),
    )

    results = _calculate_qctwo_results_batch_by_id_wrapper(
        {"qctwo_config_id": 99, "crosscorrelation_cartesian_ids": (5, 9)}
    )

    assert results == ("result-5", "result-9")
    assert called["config_ids"] == [99]
    assert called["fetched_ids"] == [(5, 9)]
    assert called["calculated"] == [(5, 99), (9, 99)]
    assert called["removed"] == 1
