"""Pipeline orchestrator — runs Bronze -> Silver -> Gold."""

import logging
import yaml

from src.pipeline.ingestion.reader import read_bronze
from src.pipeline.transformations.silver_sessions import build_silver_sessions
from src.pipeline.transformations.silver_quality import build_silver_quality
from src.pipeline.transformations.gold import build_gold
from src.pipeline.io.writer import write_outputs
from src.pipeline.models.schemas import QoSConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """
    Loads pipeline configuration from yaml file.

    Args:
        config_path: path to config.yaml

    Returns:
        dict with pipeline and qos config
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run(config_path: str = 'config.yaml') -> None:
    """
    Runs the full pipeline:
    Bronze → Silver Sessions → Silver Quality → Gold → Write Outputs

    Args:
        config_path: path to config.yaml
    """
    logger.info("Starting pipeline")

    # --- load config ---
    config = load_config(config_path)
    input_path  = config['pipeline']['input_path']
    output_path = config['pipeline']['output_path']
    qos_config  = QoSConfig(**config['qos'])

    # --- extract event date from input path ---
    # finds the first eventDate partition in the input folder
    import glob
    partitions = glob.glob(f"{input_path}/eventDate=*")
    if not partitions:
        raise FileNotFoundError(
            f"No eventDate partitions found in {input_path}"
        )
    event_date = partitions[0].split("eventDate=")[-1]
    logger.info("Processing eventDate=%s", event_date)

    # --- bronze ---
    bronze = read_bronze(input_path)

    # --- silver ---
    silver_sessions = build_silver_sessions(bronze)
    silver_quality  = build_silver_quality(bronze)

    # --- gold ---
    gold = build_gold(silver_sessions, silver_quality, qos_config)

    # --- write outputs ---
    write_outputs(
        silver_sessions,
        silver_quality,
        gold,
        output_path,
        event_date
    )

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run()
