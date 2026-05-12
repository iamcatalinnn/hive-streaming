"""Transforms Bronze to Silver session-level heartbeat data."""

import logging
import pandas as pd

from src.pipeline.models.schemas import SilverSession

logger = logging.getLogger(__name__)


def _flatten_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts server timestamp from timestampInfo struct.
    This becomes window_end — when the heartbeat was received.
    """
    df['window_end'] = df['timestampInfo'].apply(
        lambda x: x['server']
    )
    return df


def _flatten_player(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts buffering metrics from player struct.
    """
    df['bufferings'] = df['player'].apply(
        lambda x: x['bufferings']
    )
    df['buffering_time_ms'] = df['player'].apply(
        lambda x: x['bufferingTime']
    )
    return df


def _flatten_traffic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts source and p2p traffic fields from totalDistribution struct.
    """
    df['source_requests'] = df['totalDistribution'].apply(
        lambda x: x['sourceTraffic']['requests']
    )
    df['source_responses'] = df['totalDistribution'].apply(
        lambda x: x['sourceTraffic']['responses']
    )
    df['source_requested_bytes'] = df['totalDistribution'].apply(
        lambda x: x['sourceTraffic']['requestedData']
    )
    df['source_received_bytes'] = df['totalDistribution'].apply(
        lambda x: x['sourceTraffic']['receivedData']
    )
    df['p2p_requests'] = df['totalDistribution'].apply(
        lambda x: x['p2pTraffic']['requests']
    )
    df['p2p_responses'] = df['totalDistribution'].apply(
        lambda x: x['p2pTraffic']['responses']
    )
    df['p2p_requested_bytes'] = df['totalDistribution'].apply(
        lambda x: x['p2pTraffic']['requestedData']
    )
    df['p2p_received_bytes'] = df['totalDistribution'].apply(
        lambda x: x['p2pTraffic']['receivedData']
    )
    return df


def _compute_windows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes window_start as the previous heartbeat's window_end
    per viewer session, using LAG logic (shift).
    First heartbeat per viewer gets NULL — we don't know when
    their session actually started.
    """
    df = df.sort_values(
        ['customer_id', 'content_id', 'client_id', 'window_end']
    )
    df['window_start'] = df.groupby(
        ['customer_id', 'content_id', 'client_id']
    )['window_end'].shift(1)

    # Convert to nullable integer — preserves NaN as pd.NA
    # standard int can't hold NaN in pandas
    df['window_start'] = df['window_start'].astype('Int64')

    return df


def _add_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds boolean flags for data quality signals.
    has_quality_data: False for the 7 viewers with empty qualityDistribution.
    """
    df['has_quality_data'] = df['qualityDistribution'].apply(
        lambda x: len(x) > 0
    )
    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames raw camelCase columns to snake_case.
    """
    return df.rename(columns={
        'customerId': 'customer_id',
        'contentId':  'content_id',
        'clientId':   'client_id',
    })


def _select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects only the columns needed for SilverSession.
    Drops raw nested columns that have been flattened.
    """
    return df[[
        'customer_id',
        'content_id',
        'client_id',
        'window_start',
        'window_end',
        'bufferings',
        'buffering_time_ms',
        'source_requests',
        'source_responses',
        'source_requested_bytes',
        'source_received_bytes',
        'p2p_requests',
        'p2p_responses',
        'p2p_requested_bytes',
        'p2p_received_bytes',
        'has_quality_data',
    ]]


def _validate(df: pd.DataFrame) -> None:
    """
    Validates a sample of rows against the SilverSession Pydantic schema.
    Logs warnings for any rows that fail validation.
    """
    invalid_count = 0
    for _, row in df.iterrows():
        try:
            SilverSession(**row.to_dict())
        except Exception as e:
            invalid_count += 1
            logger.warning("Invalid SilverSession row: %s", e)

    if invalid_count > 0:
        logger.warning(
            "%d/%d rows failed SilverSession validation",
            invalid_count, len(df)
        )
    else:
        logger.info("All %d SilverSession rows passed validation", len(df))


def build_silver_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms Bronze DataFrame into Silver session-level heartbeat data.
    One row per viewer per 30s heartbeat window.

    Args:
        df: raw Bronze DataFrame from reader.read_bronze()

    Returns:
        pd.DataFrame conforming to SilverSession schema
    """
    logger.info("Building silver_sessions from %d Bronze rows", len(df))

    df = _rename_columns(df)
    df = _flatten_timestamps(df)
    df = _flatten_player(df)
    df = _flatten_traffic(df)
    df = _add_flags(df)
    df = _compute_windows(df)
    df = _select_columns(df)
    _validate(df)

    logger.info("silver_sessions built: %d rows", len(df))

    return df
