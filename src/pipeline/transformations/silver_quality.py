"""Transforms Bronze to Silver quality distribution data."""

import logging
import pandas as pd

from src.pipeline.models.schemas import SilverQuality

logger = logging.getLogger(__name__)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames raw camelCase columns to snake_case.
    """
    return df.rename(columns={
        'customerId': 'customer_id',
        'contentId':  'content_id',
        'clientId':   'client_id',
    })


def _extract_window_end(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts server timestamp as window_end identifier.
    """
    df['window_end'] = df['timestampInfo'].apply(
        lambda x: x['server']
    )
    return df


def _explode_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explodes qualityDistribution list into one row per quality level.
    Viewers with empty qualityDistribution are dropped here —
    they have no quality data to explode.
    """
    # filter out empty quality distributions first
    before = len(df)
    df = df[df['qualityDistribution'].apply(lambda x: len(x) > 0)]
    dropped = before - len(df)

    if dropped > 0:
        logger.warning(
            "Dropped %d rows with empty qualityDistribution", dropped
        )

    # explode — one row per quality entry
    df = df.explode('qualityDistribution').reset_index(drop=True)

    return df


def _flatten_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens each quality entry tuple into:
    - quality label (e.g. '1080p')
    - source and p2p traffic fields
    """
    df['quality'] = df['qualityDistribution'].apply(
        lambda x: x[0]
    )
    df['source_requests'] = df['qualityDistribution'].apply(
        lambda x: x[1]['sourceTraffic']['requests']
    )
    df['source_responses'] = df['qualityDistribution'].apply(
        lambda x: x[1]['sourceTraffic']['responses']
    )
    df['source_requested_bytes'] = df['qualityDistribution'].apply(
        lambda x: x[1]['sourceTraffic']['requestedData']
    )
    df['source_received_bytes'] = df['qualityDistribution'].apply(
        lambda x: x[1]['sourceTraffic']['receivedData']
    )
    df['p2p_requests'] = df['qualityDistribution'].apply(
        lambda x: x[1]['p2pTraffic']['requests']
    )
    df['p2p_responses'] = df['qualityDistribution'].apply(
        lambda x: x[1]['p2pTraffic']['responses']
    )
    df['p2p_requested_bytes'] = df['qualityDistribution'].apply(
        lambda x: x[1]['p2pTraffic']['requestedData']
    )
    df['p2p_received_bytes'] = df['qualityDistribution'].apply(
        lambda x: x[1]['p2pTraffic']['receivedData']
    )
    return df


def _select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects only the columns needed for SilverQuality.
    """
    return df[[
        'customer_id',
        'content_id',
        'client_id',
        'window_end',
        'quality',
        'source_requests',
        'source_responses',
        'source_requested_bytes',
        'source_received_bytes',
        'p2p_requests',
        'p2p_responses',
        'p2p_requested_bytes',
        'p2p_received_bytes',
    ]]


def _validate(df: pd.DataFrame) -> None:
    """
    Validates rows against SilverQuality Pydantic schema.
    Logs warnings for any rows that fail validation.
    """
    invalid_count = 0
    for _, row in df.iterrows():
        try:
            SilverQuality(**row.to_dict())
        except Exception as e:
            invalid_count += 1
            logger.warning("Invalid SilverQuality row: %s", e)

    if invalid_count > 0:
        logger.warning(
            "%d/%d rows failed SilverQuality validation",
            invalid_count, len(df)
        )
    else:
        logger.info(
            "All %d SilverQuality rows passed validation", len(df)
        )


def build_silver_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms Bronze DataFrame into Silver quality distribution data.
    One row per viewer per heartbeat window per quality level.

    Args:
        df: raw Bronze DataFrame from reader.read_bronze()

    Returns:
        pd.DataFrame conforming to SilverQuality schema
    """
    logger.info(
        "Building silver_quality from %d Bronze rows", len(df)
    )

    df = _rename_columns(df)
    df = _extract_window_end(df)
    df = _explode_quality(df)
    df = _flatten_quality(df)
    df = _select_columns(df)
    _validate(df)

    logger.info("silver_quality built: %d rows", len(df))

    return df