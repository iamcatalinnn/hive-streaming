"""Computes QoS scores and labels based on configurable thresholds."""

import logging
import pandas as pd

from src.pipeline.models.schemas import QoSConfig, QoSLabel
from typing import Optional

logger = logging.getLogger(__name__)

# quality rank mapping — higher is better
QUALITY_RANK = {
    '144p':  1,
    '270p':  2,
    '360p':  3,
    '480p':  4,
    '540p':  5,
    '720p':  6,
    '1080p': 7,
    '1440p': 8,
    '2160p': 9,
}
MAX_QUALITY_RANK = max(QUALITY_RANK.values())


def _buffering_score(
    buffering_ratio: float,
    config: QoSConfig
) -> float:
    """
    Scores buffering behaviour.
    1.0 = excellent, 0.75 = degraded, 0.25 = poor
    """
    if buffering_ratio < config.buffering_green_threshold:
        return 1.0
    elif buffering_ratio <= config.buffering_red_threshold:
        return 0.75
    else:
        return 0.25


def _quality_score(dominant_quality: Optional[str]) -> float:
    """
    Scores video quality based on dominant quality level.
    Normalized to 0-1 against max known quality rank.
    Viewers with no quality data get 0.0.
    """
    if dominant_quality is None:
        return 0.0
    rank = QUALITY_RANK.get(str(dominant_quality), 0)
    return rank / MAX_QUALITY_RANK


def _stability_score(
    quality_switches: int,
    session_duration_min: float
) -> float:
    """
    Scores stream stability based on quality switches per minute.
    1.0 = stable, 0.75 = some switching, 0.5 = unstable, 0.25 = very unstable
    """
    if session_duration_min <= 0:
        return 1.0

    switches_per_min = quality_switches / session_duration_min

    if switches_per_min < 1:
        return 1.0
    elif switches_per_min < 2:
        return 0.75
    elif switches_per_min < 4:
        return 0.5
    else:
        return 0.25


def _qos_label(qos_score: float) -> str:
    """
    Maps numeric QoS score to human readable label.
    """
    if qos_score >= 0.75:
        return QoSLabel.GREEN.value
    elif qos_score >= 0.50:
        return QoSLabel.YELLOW.value
    else:
        return QoSLabel.RED.value


def compute_qos(
    df: pd.DataFrame,
    config: QoSConfig
) -> pd.DataFrame:
    """
    Computes QoS score and label for each viewer session.

    Args:
        df:     Gold DataFrame with session metrics
        config: QoSConfig with configurable thresholds

    Returns:
        df with qos_score and qos_label columns added
    """
    logger.info("Computing QoS scores for %d sessions", len(df))

    df['buffering_score'] = df['buffering_ratio'].apply(
        lambda x: _buffering_score(x, config)
    )

    df['quality_score'] = df['dominant_quality'].apply(
        _quality_score
    )

    df['stability_score'] = df.apply(
        lambda r: _stability_score(
            r['quality_switches'],
            r['session_duration_min']
        ),
        axis=1
    )

    df['qos_score'] = (
        df['buffering_score'] * config.buffering_weight +
        df['quality_score']   * config.quality_weight +
        df['stability_score'] * config.stability_weight
    ).round(4)

    df['qos_label'] = df['qos_score'].apply(_qos_label)

    # drop intermediate score columns — not part of Gold schema
    df = df.drop(
        columns=['buffering_score', 'quality_score', 'stability_score']
    )

    # log distribution of labels
    label_counts = df['qos_label'].value_counts()
    for label, count in label_counts.items():
        logger.info(
            "QoS %s: %d viewers (%.1f%%)",
            label, count, 100 * count / len(df)
        )

    return df
