from __future__ import annotations
from typing import Literal

import polars as pl

from ggsql._ggsql import split_query, render as _render

__all__ = ["split_query", "render"]
__version__ = "0.1.0"


def render(
    df: "pl.DataFrame | pl.LazyFrame",
    viz: str,
    *,
    writer: Literal["vegalite"] = "vegalite",
) -> str:
    """Render a DataFrame with a VISUALISE spec.

    Parameters
    ----------
    df : polars.DataFrame | polars.LazyFrame
        Data to visualize. LazyFrames are collected automatically.
    viz : str
        VISUALISE spec string (e.g., "VISUALISE x, y DRAW point")
    writer : Literal["vegalite"]
        Output format. Currently only "vegalite" supported.

    Returns
    -------
    str
        Vega-Lite JSON specification.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    return _render(df, viz, writer=writer)
