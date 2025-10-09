"""
CLI modules for the monitor package.

This package contains command-line interface tools for trade setup
analysis and monitoring.
"""

from .setup_analyzer import main as setup_analyzer_main
from .hit_checker import main as hit_checker_main

__all__ = [
    "setup_analyzer_main",
    "hit_checker_main",
]