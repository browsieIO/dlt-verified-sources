"""
This source provides data extraction via Browsie API.
Available resources: [endpoint1, endpoint2]
"""

from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from .helpers import run_job


@dlt.resource(write_disposition="replace")
def job(spec) -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield run_job(spec)


# @dlt.resource(write_disposition="replace")
# def pokemon() -> Iterable[TDataItem]:
#     """
#     Returns a list of pokemon.
#     Yields:
#         dict: The pokemon data.
#     """
#     yield requests.get(POKEMON_URL).json()["results"]


@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [job]
