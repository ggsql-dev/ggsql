from __future__ import annotations
from typing import Literal, overload

import altair
import narwhals as nw
from narwhals.typing import IntoFrame

from ggsql._ggsql import split_query, render as _render

__all__ = ["split_query", "render"]
__version__ = "0.1.0"

_SUPPORTED_WRITERS = {"vegalite"}


@overload
def render(
    df: IntoFrame,
    viz: str,
    *,
    writer: Literal["vegalite"] = ...,
) -> altair.Chart: ...


def render(
    df: IntoFrame,
    viz: str,
    *,
    writer: Literal["vegalite"] = "vegalite",
) -> altair.Chart:
    """Render a DataFrame with a VISUALISE spec.

    Parameters
    ----------
    df
        Data to visualize. Accepts polars, pandas, or any narwhals-compatible
        DataFrame. LazyFrames are collected automatically.
    viz
        VISUALISE spec string (e.g., "VISUALISE x, y DRAW point")
    writer
        Output format. Currently only "vegalite" supported.

    Returns
    -------
    altair.Chart
        An Altair chart object.
    """
    if writer not in _SUPPORTED_WRITERS:
        raise ValueError(
            f"Unknown writer: {writer!r}. Supported writers: {', '.join(sorted(_SUPPORTED_WRITERS))}"
        )

    df = nw.from_native(df, pass_through=True)

    if isinstance(df, nw.LazyFrame):
        df = df.collect()

    if not isinstance(df, nw.DataFrame):
        raise TypeError("df must be a narwhals DataFrame or compatible type")

    # Should be safe as long as we take polars dependency
    pl_df = df.to_polars()

    vegalite_json = _render(pl_df, viz, writer=writer)

    return altair.Chart.from_json(vegalite_json)
