"""
Logging for zensus2pgsql

Uses the rich.logging.RichHandler for colorful logs

More information:
    https://rich.readthedocs.io/en/stable/logging.html
"""

import logging

from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level=logging.WARNING,  # Default to WARNING level
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(markup=True)],
)

logger = logging.getLogger("zensus2pgsql")


def configure_logging(verbosity: int, quiet: bool = False) -> None:
    """Configure logging level based on verbosity count.

    Args:
        verbosity: Number of -v flags passed (0, 1, 2, 3+)
            0: WARNING
            1: INFO
            2: DEBUG
            3+: DEBUG (same as 2)
        quiet: If True (default), suppress logging messages
    """
    if quiet:
        level = logging.ERROR
    elif verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    else:  # 2 or more
        level = logging.DEBUG

    # Set level on both the specific logger and root logger
    logger.setLevel(level)
    logging.getLogger().setLevel(level)
