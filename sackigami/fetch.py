from typing import Optional

import nflreadpy as nfl
import polars as pl

from sackigami.constants import (
    COL,
    DATA_OF_INTEREST,
    STAT_THRESHOLDS,
)


def retrieve_complete_team_stats() -> pl.DataFrame:
    return nfl.load_team_stats(seasons=True, summary_level="week")


def parse_last_gameday(complete_team_stats: pl.DataFrame) -> dict[str, int]:
    last_season: int = complete_team_stats.select(COL.season.last()).item()
    last_week: int = complete_team_stats.select(COL.week.last()).item()
    return {
        "season": last_season,
        "week": last_week,
    }


def retrieve_week(
    complete_team_stats: pl.DataFrame, week: Optional[dict[str, int]] = None
) -> pl.DataFrame:
    gameday: dict[str, int] = (
        parse_last_gameday(complete_team_stats) if week is None else week
    )

    return complete_team_stats.filter(
        (COL.season == gameday["season"]) & (COL.week == gameday["week"])
    )


def parse_sack_data(weekly_stat_lines: pl.DataFrame) -> pl.DataFrame:
    return weekly_stat_lines.select([data for data in DATA_OF_INTEREST])


def filter_by_threshold(stat_lines: pl.DataFrame) -> pl.DataFrame:
    return stat_lines.filter(
        (COL.sacks_suffered >= STAT_THRESHOLDS["sacks_suffered"])
        | (COL.sack_yards_lost <= STAT_THRESHOLDS["sack_yards_lost"])
        | (COL.sack_fumbles >= STAT_THRESHOLDS["sack_fumbles"])
        | (COL.sack_fumbles_lost >= STAT_THRESHOLDS["sack_fumbles_lost"])
    )


def find_similar_stat_lines(
    complete_team_stats: pl.DataFrame, stat_line: pl.DataFrame | dict[str, str | int]
) -> Optional[dict[str, int]]:

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

    return {
        "count": count,
        "week": last_week,
        "season": last_season,
    }
