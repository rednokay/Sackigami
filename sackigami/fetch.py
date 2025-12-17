from typing import Optional

import nflreadpy as nfl
import polars as pl

from constants import (
    STAT_THRESHOLDS,
    DATA_OF_INTEREST,
    TEAMS,
    COL_OPPONENT_TEAM,
    COL_SACK_FUMBLES,
    COL_SACK_FUMBLES_LOST,
    COL_SACKS_SUFFERED,
    COL_SACK_YARDS_LOST,
    COL_SEASON,
    COL_WEEK,
    COL_TEAM,
)


def retrieve_complete_team_stats() -> pl.DataFrame:
    return nfl.load_team_stats(seasons=True, summary_level="week")


def parse_last_gameday(complete_team_stats: pl.DataFrame) -> dict[str, int]:
    last_season: int = complete_team_stats.select(COL_SEASON.last()).item()
    last_week: int = complete_team_stats.select(COL_WEEK.last()).item()
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
        (COL_SEASON == gameday["season"]) & (COL_WEEK == gameday["week"])
    )


def parse_sack_data(weekly_stat_lines: pl.DataFrame) -> pl.DataFrame:
    return weekly_stat_lines.select([data for data in DATA_OF_INTEREST])


def filter_by_threshold(stat_lines: pl.DataFrame) -> pl.DataFrame:
    return stat_lines.filter(
        (COL_SACKS_SUFFERED >= STAT_THRESHOLDS["sacks_suffered"])
        | (COL_SACK_YARDS_LOST <= STAT_THRESHOLDS["sack_yards_lost"])
        | (COL_SACK_FUMBLES >= STAT_THRESHOLDS["sack_fumbles"])
        | (COL_SACK_FUMBLES_LOST >= STAT_THRESHOLDS["sack_fumbles_lost"])
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
            sacks_suffered = stat_line.select(COL_SACKS_SUFFERED.last()).item()
            sack_yards_lost = stat_line.select(COL_SACK_YARDS_LOST.last()).item()
            sack_fumbles = stat_line.select(COL_SACK_FUMBLES.last()).item()
            sack_fumbles_lost = stat_line.select(COL_SACK_FUMBLES_LOST.last()).item()
            season = stat_line.select(COL_SEASON.last()).item()
            week = stat_line.select(COL_WEEK.last()).item()
            team = stat_line.select(COL_TEAM.last()).item()
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
        (COL_SACKS_SUFFERED == sacks_suffered)
        & (COL_SACK_YARDS_LOST == sack_yards_lost)
        & (COL_SACK_FUMBLES == sack_fumbles)
        & (COL_SACK_FUMBLES_LOST == sack_fumbles_lost)
    )

    similar_lines = similar_lines.filter(
        ~((COL_TEAM == team) & (COL_SEASON == season) & (COL_WEEK == week))
    )

    count: int = similar_lines.height

    if count == 0:
        return None

    last_week: int = similar_lines.select(COL_WEEK.last()).item()
    last_season: int = similar_lines.select(COL_SEASON.last()).item()

    return {
        "count": count,
        "week": last_week,
        "season": last_season,
    }


# TODO: Pluralize
def print_stat_line(stat_line: pl.DataFrame, complete_team_stats: pl.DataFrame) -> None:
    data = {
        item: stat_line.select(pl.col(item).last()).item() for item in DATA_OF_INTEREST
    }

    prev: Optional[dict[str, int]] = find_similar_stat_lines(
        complete_team_stats, stat_line
    )

    print(
        f"The {TEAMS[data["team"]]} suffered {data["sacks_suffered"]} sacks in their game against the {TEAMS[data["opponent_team"]]} today. "
        + f"This resulted in {abs(data["sack_yards_lost"])} yards lost. "
        + f"{data["sack_fumbles"]} of those sacks were strip-sacks resulting in {data["sack_fumbles_lost"]} turnovers."
    )

    if prev is not None:
        print(
            f'No Sackigami! This stat line happened {prev["count"]} times before. '
            + f'Most recently in week {prev["week"]} of {prev["season"]}.'
        )
