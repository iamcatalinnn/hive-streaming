"""
Tests for silver_quality transformation.
"""


def test_empty_quality_rows_dropped(sample_bronze):
    """
    viewer-3 has empty qualityDistribution.
    silver_quality should drop that row — no quality data to explode.
    """
    from src.pipeline.transformations.silver_quality import build_silver_quality
    sq = build_silver_quality(sample_bronze)

    assert 'viewer-3' not in sq['client_id'].values


def test_explode_produces_correct_row_count(silver_quality):
    """
    viewer-1: 3 heartbeats × 1 quality each = 3 rows
    viewer-2: 2 heartbeats × 1 quality each = 2 rows
    viewer-3: dropped (empty)
    Total: 5 rows
    """
    assert len(silver_quality) == 5


def test_quality_values_are_valid(silver_quality):
    """
    All quality values should be valid VideoQuality enum values.
    Our fixture uses 1080p and 720p — both valid.
    """
    from src.pipeline.models.schemas import VideoQuality
    valid_qualities = {q.value for q in VideoQuality}

    for quality in silver_quality['quality'].unique():
        assert quality in valid_qualities


def test_traffic_fields_per_quality(silver_quality):
    """
    Traffic fields should be correctly extracted per quality level.
    viewer-1 first heartbeat at 1080p has source_received_bytes=6000.
    """
    row = silver_quality[
        (silver_quality['client_id'] == 'viewer-1') &
        (silver_quality['window_end'] == 1000000) &
        (silver_quality['quality'] == '1080p')
    ].iloc[0]

    assert row['source_received_bytes'] == 6000
    assert row['p2p_received_bytes'] == 2000


def test_window_end_matches_silver_sessions(silver_sessions, silver_quality):
    """
    window_end in silver_quality should match window_end in silver_sessions.
    This validates the join key is consistent across both Silver tables.
    """
    session_windows = set(
        silver_sessions['window_end'].unique()
    )
    quality_windows = set(
        silver_quality['window_end'].unique()
    )

    # all quality windows should exist in sessions
    assert quality_windows.issubset(session_windows)
