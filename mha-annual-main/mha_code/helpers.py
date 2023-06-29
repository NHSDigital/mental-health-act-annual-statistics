import os
import sys
import toml
import time
from pathlib import Path
from typing import Dict
import logging

logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Return the project root path from any file in the project.

    Example:
        from shmi_improvement.utilities.helpers import get_project_root
        root_path = get_project_root()
    """
    return Path(__file__).parent.parent


def get_excel_template_dir() -> Path:
    """
    """
    return Path(__file__).parent.parent / 'excel_templates'

def get_config() -> Dict:
    """Gets the config toml from the root directory and returns it as a dict. Can be called from any file in the project

    Returns:
        Dict: A dictionary containing details of the database, paths, etc. Should contain all the things that will change from one run to the next/

    Example:
        from shmi_improvement.utilities.helpers import get_config
        config = get_config()
    """
    root_path = get_project_root()
    return toml.load(root_path / "config.toml")


def configure_logging(log_dir) -> None:
    """Set up logging format and location to store logs
    Should move path to config
    """
    log_folder = log_dir
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s -- %(filename)s:\
                %(funcName)5s():%(lineno)s -- %(message)s',
        handlers=[
            logging.FileHandler(log_folder / f"{time.strftime('%Y-%m-%d_%H-%M-%S')}.log"),
            logging.StreamHandler(sys.stdout)]  # Add second handler to print log message to screen
    )
