"""
Data Pipeline Module

A flexible ETL (Extract, Transform, Load) pipeline for processing data
from various sources through customizable transformations to target destinations.
"""

__version__ = "0.1.0"
__author__ = "Data Pipeline Team"

from .pipeline.pipeline import DataPipeline
from .pipeline.config import Config

__all__ = ["DataPipeline", "Config"]