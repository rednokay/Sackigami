from typing import Optional
from dataclasses import dataclass

import nflreadpy as nfl
import polars as pl

from sackigami.constants import (
    COL,
    DATA_OF_INTEREST,
    STAT_THRESHOLDS,
)


@dataclass
class GameDay:
    season: int
    """Season or year of the game day.

    This is always the year in which the season begins in,
    even after January 1st.
    """

    week: int
    """Week of the game day."""


@dataclass
class SimilarStatLines:
    last_gameday: GameDay
    """Gameday the a similar stat line occured the last time."""

    count: int
    """How often the similar stat line occured."""


def retrieve_complete_team_stats() -> pl.DataFrame:
    """Download complete teams stats of all available seasons.

    Returns:
        pl.DataFrame: Complete team stats.
    """
    return nfl.load_team_stats(seasons=True, summary_level="week")


def parse_last_gameday(complete_team_stats: pl.DataFrame) -> GameDay:
    """Returns the latest/last/current game day.

    Args:
        complete_team_stats (pl.DataFrame): Complete team stats.

    Returns:
        GameDay: Last game day in the data.
    """
    last_season: int = complete_team_stats.select(COL.season.last()).item()
    last_week: int = complete_team_stats.select(COL.week.last()).item()
    return GameDay(last_season, last_week)


def retrieve_weekly_stats(
    complete_team_stats: pl.DataFrame, gameday: Optional[GameDay] = None
) -> pl.DataFrame:
    """Retrieves the team stats for a given gameday.

    Args:
        complete_team_stats (pl.DataFrame): Complete team stats.
        gameday (Optional[GameDay], optional): Game day of which to retrieve data. If none, the latest is chosen. Defaults to None.

    Returns:
        pl.DataFrame: Weekly team stats.
    """

    gameday_conditional: GameDay = (
        parse_last_gameday(complete_team_stats) if gameday is None else gameday
    )

    return complete_team_stats.filter(
        (COL.season == gameday_conditional.season)
        & (COL.week == gameday_conditional.week)
    )


def parse_sack_data(weekly_team_stats: pl.DataFrame) -> pl.DataFrame:
    """Select relevant columns from given team stats.

    Args:
        weekly_team_stats (pl.DataFrame): Weekly team stats.

    Returns:
        pl.DataFrame: Selected columns.
    """
    return weekly_team_stats.select([data for data in DATA_OF_INTEREST])


def find_similar_stat_lines(
    complete_team_stats: pl.DataFrame, stat_line: pl.DataFrame | dict[str, str | int]
) -> Optional[SimilarStatLines]:
    """Finds the amount of and the last time a completely similar stat line occured.

    Args:
        complete_team_stats (pl.DataFrame): Complete team stats.
        stat_line (pl.DataFrame | dict[str, str  |  int]): The stat line to look for.

    Raises:
        TypeError: Stat line is of invalid data type.

    Returns:
        Optional[SimilarStatLines]: The similar stat line or None of none found.
    """

    sacks_suffered: Optional[int] = None
    sack_yards_lost: Optional[int] = None
    sack_fumbles: Optional[int] = None
    sack_fumbles_lost: Optional[int] = None
    season: Optional[int] = None
    week: Optional[int] = None
    team: Optional[str] = None

    match stat_line:
        case pl.DataFrame():
            sacks_suffered = stat_line.select(COL.sacks_suffered.last()).item()
            sack_yards_lost = stat_line.select(COL.sack_yards_lost.last()).item()
            sack_fumbles = stat_line.select(COL.sack_fumbles.last()).item()
            sack_fumbles_lost = stat_line.select(COL.sack_fumbles_lost.last()).item()
            season = stat_line.select(COL.season.last()).item()
            week = stat_line.select(COL.week.last()).item()
            team = stat_line.select(COL.team.last()).item()
        case dict():
            sacks_suffered = int(stat_line["sacks_suffered"])
            sack_yards_lost = int(stat_line["sack_yards_lost"])
            sack_fumbles = int(stat_line["sack_fumbles"])
            sack_fumbles_lost = int(stat_line["sack_fumbles_lost"])
            season = int(stat_line["season"])
            week = int(stat_line["week"])
            team = str(stat_line["team"])
        case _:
            raise TypeError(f"Unhandled type for {complete_team_stats}")

    similar_lines: pl.DataFrame = complete_team_stats.filter(
        (COL.sacks_suffered == sacks_suffered)
        & (COL.sack_yards_lost == sack_yards_lost)
        & (COL.sack_fumbles == sack_fumbles)
        & (COL.sack_fumbles_lost == sack_fumbles_lost)
    )

    similar_lines = similar_lines.filter(
        ~((COL.team == team) & (COL.season == season) & (COL.week == week))
    )

    count: int = similar_lines.height

    if count == 0:
        return None

    last_week: int = similar_lines.select(COL.week.last()).item()
    last_season: int = similar_lines.select(COL.season.last()).item()

    return SimilarStatLines(GameDay(last_season, last_week), count)
