"""
Tests for gold transformation.
"""

import pytest


@pytest.fixture
def gold(silver_sessions, silver_quality, default_qos_config):
    """Gold built from silver fixtures."""
    from src.pipeline.transformations.gold import build_gold
    return build_gold(silver_sessions, silver_quality, default_qos_config)


def test_gold_row_count(gold):
    """
    One Gold row per unique viewer session.
    We have 3 viewers → 3 Gold rows.
    """
    assert len(gold) == 3


def test_session_start_is_min_window_end(gold, silver_sessions):
    """
    session_start should be MIN(window_end) per viewer —
    the earliest heartbeat timestamp we observed.
    This validates we don't lose the first heartbeat.
    """
    viewer_1_gold = gold[gold['client_id'] == 'viewer-1'].iloc[0]
    viewer_1_silver = silver_sessions[
        silver_sessions['client_id'] == 'viewer-1'
    ]

    assert viewer_1_gold['session_start'] == viewer_1_silver['window_end'].min()


def test_total_bufferings_is_sum(gold, silver_sessions):
    """
    total_bufferings should be SUM of bufferings across all heartbeats.
    viewer-1: 2 + 1 + 0 = 3
    """
    viewer_1_gold = gold[gold['client_id'] == 'viewer-1'].iloc[0]
    assert viewer_1_gold['total_bufferings'] == 3


def test_total_buffering_time_is_sum(gold, silver_sessions):
    """
    total_buffering_time_ms should be SUM across all heartbeats.
    viewer-1: 5000 + 2000 + 0 = 7000
    """
    viewer_1_gold = gold[gold['client_id'] == 'viewer-1'].iloc[0]
    assert viewer_1_gold['total_buffering_time_ms'] == 7000


def test_quality_switches_counts_cross_window_only(gold):
    """
    viewer-1 switches: 1080p → 720p → 1080p = 2 cross-window switches.
    Should NOT count intra-window quality rows as switches.
    """
    viewer_1_gold = gold[gold['client_id'] == 'viewer-1'].iloc[0]
    assert viewer_1_gold['quality_switches'] == 2


def test_no_quality_switches_for_stable_viewer(gold):
    """
    viewer-2 watches 1080p consistently across all heartbeats.
    Should have 0 quality switches.
    """
    viewer_2_gold = gold[gold['client_id'] == 'viewer-2'].iloc[0]
    assert viewer_2_gold['quality_switches'] == 0


def test_dominant_quality_is_highest_bytes(gold):
    """
    viewer-1 has 1080p in windows 1 and 3, 720p in window 2.
    1080p has more total received bytes → dominant quality = 1080p.
    """
    viewer_1_gold = gold[gold['client_id'] == 'viewer-1'].iloc[0]
    assert viewer_1_gold['dominant_quality'] == '1080p'


def test_dominant_quality_none_for_empty_quality(gold):
    """
    viewer-3 has no quality data.
    dominant_quality should be None.
    """
    viewer_3_gold = gold[gold['client_id'] == 'viewer-3'].iloc[0]
    assert viewer_3_gold['dominant_quality'] is None
