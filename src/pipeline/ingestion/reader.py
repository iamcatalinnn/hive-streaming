"""Reads and validates Bronze Delta/parquet input data."""

import glob
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def read_bronze(input_path: str) -> pd.DataFrame:
    """
    Loads all parquet files from Hive-partitioned Delta table folder.
    Excludes _delta_log metadata files.

    Args:
        input_path: path to the root Delta table folder e.g. 'data/'

    Returns:
        pd.DataFrame with raw Bronze data

    Raises:
        FileNotFoundError: if no parquet files are found at input_path
    """
    pattern = f"{input_path.rstrip('/')}/eventDate=*/*.parquet"
    files = glob.glob(pattern)

    if not files:
        logger.error("No parquet files found at path: %s", input_path)
        raise FileNotFoundError(
            f"No parquet files found at path: {input_path}"
        )

    logger.info("Found %d parquet files in %s", len(files), input_path)

    frames = []
    for file in files:
        try:
            frames.append(pd.read_parquet(file))
        except Exception as e:
            logger.warning("Skipping file %s — failed to read: %s", file, e)

    if not frames:
        logger.error("All files failed to read in path: %s", input_path)
        raise RuntimeError(
            f"All parquet files failed to read in: {input_path}"
        )

    df = pd.concat(frames, ignore_index=True)

    logger.info("Successfully loaded %d rows from %s", len(df), input_path)

    return df
