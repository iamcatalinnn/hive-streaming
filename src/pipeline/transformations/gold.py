"""Aggregates Silver tables into Gold session-level metrics."""

import logging
import pandas as pd
import numpy as np

from src.pipeline.models.schemas import Gold, VideoQuality, QoSConfig
from src.pipeline.quality.qos import compute_qos

logger = logging.getLogger(__name__)


def _aggregate_sessions(silver_sessions: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates silver_sessions to one row per viewer session.
    Computes session timing and buffering metrics.
    """
    grp = silver_sessions.groupby(
        ['customer_id', 'content_id', 'client_id']
    )

    agg = pd.DataFrame({
        'session_start':                grp['window_end'].min(),
        'session_end':                  grp['window_end'].max(),
        'total_bufferings':             grp['bufferings'].sum(),
        'total_buffering_time_ms':      grp['buffering_time_ms'].sum(),
        'total_source_requested_bytes': grp['source_requested_bytes'].sum(),
        'total_source_received_bytes':  grp['source_received_bytes'].sum(),
        'total_p2p_requested_bytes':    grp['p2p_requested_bytes'].sum(),
        'total_p2p_received_bytes':     grp['p2p_received_bytes'].sum(),
    }).reset_index()

    return agg


def _compute_session_metrics(agg: pd.DataFrame) -> pd.DataFrame:
    """
    Derives session-level metrics from aggregated values.
    """
    duration_ms = agg['session_end'] - agg['session_start']

    agg['session_duration_min'] = duration_ms / 60000

    # buffering ratio — protect against zero duration
    agg['buffering_ratio'] = agg.apply(
        lambda r: r['total_buffering_time_ms'] / (
            r['session_end'] - r['session_start']
        ) if r['session_end'] != r['session_start'] else 0.0,
        axis=1
    )

    # cap at 1.0 — buffering can't exceed session duration
    agg['buffering_ratio'] = agg['buffering_ratio'].clip(upper=1.0)

    # delivery rate — were requests fulfilled?
    total_requested = (
        agg['total_source_requested_bytes'] +
        agg['total_p2p_requested_bytes']
    )
    total_received = (
        agg['total_source_received_bytes'] +
        agg['total_p2p_received_bytes']
    )

    agg['delivery_rate'] = agg.apply(
        lambda r: total_received[r.name] / total_requested[r.name]
        if total_requested[r.name] > 0 else 1.0,
        axis=1
    ).clip(upper=1.0)

    # p2p ratio
    agg['p2p_ratio'] = agg.apply(
        lambda r: r['total_p2p_received_bytes'] / total_received[r.name]
        if total_received[r.name] > 0 else 0.0,
        axis=1
    ).clip(0.0, 1.0)

    return agg


def _compute_quality_metrics(
    agg: pd.DataFrame,
    silver_quality: pd.DataFrame
) -> pd.DataFrame:
    """
    Computes dominant quality and quality switches per viewer session.
    Dominant quality per window is computed first to avoid
    counting intra-window quality rows as switches.
    """
    sq = silver_quality.copy()
    sq['total_bytes'] = (
        sq['source_received_bytes'] + sq['p2p_received_bytes']
    )

    # --- dominant quality per window ---
    # one representative quality per window = quality with most received bytes
    window_quality = sq.loc[
        sq.groupby(
            ['customer_id', 'content_id', 'client_id', 'window_end']
        )['total_bytes'].idxmax()
    ][['customer_id', 'content_id', 'client_id', 'window_end', 'quality']]

    # --- dominant quality per session ---
    # quality with most total bytes across all windows
    quality_bytes = sq.groupby(
        ['customer_id', 'content_id', 'client_id', 'quality']
    )['total_bytes'].sum().reset_index()

    dominant = quality_bytes.loc[
        quality_bytes.groupby(
            ['customer_id', 'content_id', 'client_id']
        )['total_bytes'].idxmax()
    ][['customer_id', 'content_id', 'client_id', 'quality']].rename(
        columns={'quality': 'dominant_quality'}
    )

    # --- quality switches across windows only ---
    wq_sorted = window_quality.sort_values(
        ['customer_id', 'content_id', 'client_id', 'window_end']
    )

    wq_sorted['prev_quality'] = wq_sorted.groupby(
        ['customer_id', 'content_id', 'client_id']
    )['quality'].shift(1)

    wq_sorted['is_switch'] = (
        wq_sorted['quality'] != wq_sorted['prev_quality']
    ) & wq_sorted['prev_quality'].notna()

    switches = wq_sorted.groupby(
        ['customer_id', 'content_id', 'client_id']
    )['is_switch'].sum().reset_index().rename(
        columns={'is_switch': 'quality_switches'}
    )

    # --- join onto aggregated sessions ---
    agg = agg.merge(
        dominant,
        on=['customer_id', 'content_id', 'client_id'],
        how='left'
    )
    agg = agg.merge(
        switches,
        on=['customer_id', 'content_id', 'client_id'],
        how='left'
    )

    agg['dominant_quality'] = agg['dominant_quality'].where(
        agg['dominant_quality'].notna(), None
    )
    agg['quality_switches'] = agg['quality_switches'].fillna(0).astype(int)

    return agg


def _validate(df: pd.DataFrame) -> None:
    """
    Validates rows against Gold Pydantic schema.
    """
    invalid_count = 0
    for _, row in df.iterrows():
        try:
            Gold(**row.to_dict())
        except Exception as e:
            invalid_count += 1
            logger.warning("Invalid Gold row: %s", e)

    if invalid_count > 0:
        logger.warning(
            "%d/%d rows failed Gold validation",
            invalid_count, len(df)
        )
    else:
        logger.info(
            "All %d Gold rows passed validation", len(df)
        )


def build_gold(
    silver_sessions: pd.DataFrame,
    silver_quality: pd.DataFrame,
    config: QoSConfig
) -> pd.DataFrame:
    """
    Aggregates Silver tables into Gold session-level metrics.
    One row per viewer session with full QoS analysis.

    Args:
        silver_sessions: output of build_silver_sessions()
        silver_quality:  output of build_silver_quality()

    Returns:
        pd.DataFrame conforming to Gold schema
    """
    logger.info(
        "Building gold from %d session rows and %d quality rows",
        len(silver_sessions), len(silver_quality)
    )

    agg = _aggregate_sessions(silver_sessions)
    agg = _compute_session_metrics(agg)
    agg = _compute_quality_metrics(agg, silver_quality)
    agg = compute_qos(agg, config)
    _validate(agg)

    logger.info("gold built: %d rows", len(agg))

    return agg
