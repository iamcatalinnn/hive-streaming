"""
Tests for silver_sessions transformation.
Each test covers one specific behaviour we designed.
"""

import pandas as pd
import pytest


def test_row_count(silver_sessions):
    """
    All Bronze rows should produce one Silver session row each.
    6 Bronze rows → 6 Silver session rows.
    """
    assert len(silver_sessions) == 6


def test_first_heartbeat_window_start_is_null(silver_sessions):
    """
    The first heartbeat per viewer should have NULL window_start
    because we don't know when their session actually started.
    We have 3 viewers → 3 first heartbeats → 3 NULLs.
    """
    null_count = silver_sessions['window_start'].isna().sum()
    assert null_count == 3


def test_window_start_equals_previous_window_end(silver_sessions):
    """
    For non-first heartbeats, window_start should equal
    the previous heartbeat's window_end.
    This validates our LAG logic is working correctly.
    """
    viewer_1 = silver_sessions[
        silver_sessions['client_id'] == 'viewer-1'
    ].sort_values('window_end').reset_index(drop=True)

    # second heartbeat's window_start = first heartbeat's window_end
    assert viewer_1.loc[1, 'window_start'] == viewer_1.loc[0, 'window_end']
    # third heartbeat's window_start = second heartbeat's window_end
    assert viewer_1.loc[2, 'window_start'] == viewer_1.loc[1, 'window_end']


def test_buffering_fields_correctly_extracted(silver_sessions):
    """
    Buffering fields should be correctly extracted from player struct.
    viewer-1's first heartbeat has bufferings=2, bufferingTime=5000.
    """
    viewer_1_first = silver_sessions[
        (silver_sessions['client_id'] == 'viewer-1') &
        (silver_sessions['window_end'] == 1000000)
    ].iloc[0]

    assert viewer_1_first['bufferings'] == 2
    assert viewer_1_first['buffering_time_ms'] == 5000


def test_traffic_fields_correctly_extracted(silver_sessions):
    """
    Traffic fields should be correctly extracted from totalDistribution.
    """
    viewer_1_first = silver_sessions[
        (silver_sessions['client_id'] == 'viewer-1') &
        (silver_sessions['window_end'] == 1000000)
    ].iloc[0]

    assert viewer_1_first['source_requests'] == 5
    assert viewer_1_first['source_received_bytes'] == 10000
    assert viewer_1_first['p2p_requests'] == 2
    assert viewer_1_first['p2p_received_bytes'] == 4000


def test_has_quality_data_flag(silver_sessions):
    """
    viewer-3 has empty qualityDistribution.
    has_quality_data should be False for viewer-3
    and True for all others.
    """
    viewer_3 = silver_sessions[
        silver_sessions['client_id'] == 'viewer-3'
    ].iloc[0]

    assert viewer_3['has_quality_data'] == False

    others = silver_sessions[
        silver_sessions['client_id'] != 'viewer-3'
    ]
    assert others['has_quality_data'].all()


def test_columns_renamed_to_snake_case(silver_sessions):
    """
    Raw camelCase columns should be renamed to snake_case.
    """
    assert 'customer_id' in silver_sessions.columns
    assert 'content_id' in silver_sessions.columns
    assert 'client_id' in silver_sessions.columns
    assert 'customerId' not in silver_sessions.columns
    assert 'contentId' not in silver_sessions.columns
    assert 'clientId' not in silver_sessions.columns
