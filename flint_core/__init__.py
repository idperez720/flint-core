"""Flint Core: An elite, agnostic Python framework to standardize data engineering pipelines."""

import logging

from flint_core.functions import deduplication, scd2

__version__ = "0.3.3"

logger = logging.getLogger(__name__)

# 1. Trigger optional backend engine auto-registration via side-effects
try:
    import flint_core.pandas_core  # noqa: F401

    logger.debug("Successfully initialized flint-core pandas backend.")
except ImportError:
    logger.debug("Pandas backend optional dependencies not found. Skipping auto-registration.")

try:
    import flint_core.spark_core  # noqa: F401

    logger.debug("Successfully initialized flint-core spark backend.")
except ImportError:
    logger.debug("Spark backend optional dependencies not found. Skipping auto-registration.")


# 2. PEP 484 EXPLICIT RE-EXPORTS (The IDE Fix)
# The 'as deduplication' syntax explicitly exposes these packages to Pylance/Mypy,
# unlocking 100% perfect autocomplete while keeping files cleanly hidden inside core/


__all__ = ["deduplication", "scd2"]
