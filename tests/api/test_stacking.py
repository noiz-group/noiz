from noiz.api.stacking import _prepare_stacking_parallel_task_inputs


def test_prepare_stacking_parallel_task_inputs_chunks_inputs():
    inputs = (
        {"qctwo_config_id": 1, "componentpair_cartesian_id": 10, "stacking_schema_id": 2, "stacking_timespan_id": 3},
        {"qctwo_config_id": 1, "componentpair_cartesian_id": 11, "stacking_schema_id": 2, "stacking_timespan_id": 3},
        {"qctwo_config_id": 1, "componentpair_cartesian_id": 12, "stacking_schema_id": 2, "stacking_timespan_id": 3},
        {"qctwo_config_id": 1, "componentpair_cartesian_id": 13, "stacking_schema_id": 2, "stacking_timespan_id": 3},
        {"qctwo_config_id": 1, "componentpair_cartesian_id": 14, "stacking_schema_id": 2, "stacking_timespan_id": 3},
    )

    chunked = _prepare_stacking_parallel_task_inputs(inputs, per_task_chunk_size=2)

    assert chunked == [
        (
            {
                "qctwo_config_id": 1,
                "componentpair_cartesian_id": 10,
                "stacking_schema_id": 2,
                "stacking_timespan_id": 3,
            },
            {
                "qctwo_config_id": 1,
                "componentpair_cartesian_id": 11,
                "stacking_schema_id": 2,
                "stacking_timespan_id": 3,
            },
        ),
        (
            {
                "qctwo_config_id": 1,
                "componentpair_cartesian_id": 12,
                "stacking_schema_id": 2,
                "stacking_timespan_id": 3,
            },
            {
                "qctwo_config_id": 1,
                "componentpair_cartesian_id": 13,
                "stacking_schema_id": 2,
                "stacking_timespan_id": 3,
            },
        ),
        (
            {
                "qctwo_config_id": 1,
                "componentpair_cartesian_id": 14,
                "stacking_schema_id": 2,
                "stacking_timespan_id": 3,
            },
        ),
    ]
