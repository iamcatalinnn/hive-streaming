"""Writes Silver and Gold outputs to partitioned parquet files."""

import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


def _write_partitioned(
    df: pd.DataFrame,
    output_path: str,
    table_name: str,
    event_date: str
) -> None:
    """
    Writes a DataFrame to a partitioned parquet file.
    Output structure: output_path/table_name/eventDate=YYYY-MM-DD/part-0.parquet

    Args:
        df:          DataFrame to write
        output_path: root output directory
        table_name:  name of the table (silver_sessions, silver_quality, gold)
        event_date:  partition value e.g. '2025-11-13'
    """
    partition_path = os.path.join(
        output_path,
        table_name,
        f"eventDate={event_date}"
    )

    os.makedirs(partition_path, exist_ok=True)

    file_path = os.path.join(partition_path, "part-0.parquet")

    # convert Int64 nullable integer to standard int64 before writing
    # pyarrow doesn't handle pandas Int64 well in older versions
    df = df.copy()
    for col in df.select_dtypes(include='Int64').columns:
        df[col] = df[col].astype('float64')

    df.to_parquet(file_path, index=False)

    logger.info(
        "Written %d rows to %s",
        len(df), file_path
    )


def write_outputs(
    silver_sessions: pd.DataFrame,
    silver_quality: pd.DataFrame,
    gold: pd.DataFrame,
    output_path: str,
    event_date: str
) -> None:
    """
    Writes all pipeline outputs to partitioned parquet files.

    Args:
        silver_sessions: output of build_silver_sessions()
        silver_quality:  output of build_silver_quality()
        gold:            output of build_gold()
        output_path:     root output directory e.g. 'output/'
        event_date:      partition date e.g. '2025-11-13'
    """
    logger.info(
        "Writing outputs to %s for eventDate=%s",
        output_path, event_date
    )

    _write_partitioned(
        silver_sessions, output_path, 'silver_sessions', event_date
    )
    _write_partitioned(
        silver_quality, output_path, 'silver_quality', event_date
    )
    _write_partitioned(
        gold, output_path, 'gold', event_date
    )

    logger.info("All outputs written successfully")