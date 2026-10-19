"""Flint Core: An elite, agnostic Python framework to standardize data engineering pipelines."""

import logging

from flint_core.runtime import FlintProject, Node, Output, Pipeline, WritePolicy

__version__ = "0.5.0"

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


__all__ = ["FlintProject", "Node", "Output", "Pipeline", "WritePolicy"]
