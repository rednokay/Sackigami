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


def retrieve_last_week(complete_team_stats: pl.DataFrame) -> pl.DataFrame:
    last_season: int = complete_team_stats.select(COL_SEASON.last()).item()
    last_week: int = complete_team_stats.select(COL_WEEK.last()).item()

    return complete_team_stats.filter(
        (COL_SEASON == last_season) & (COL_WEEK == last_week)
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
    complete_team_stats: pl.DataFrame, stat_line: pl.DataFrame
) -> Optional[dict[str, int]]:
    similar_lines: pl.DataFrame = complete_team_stats.filter(
        (COL_SACKS_SUFFERED == stat_line.select(COL_SACKS_SUFFERED.last()).item())
        & (COL_SACK_YARDS_LOST == stat_line.select(COL_SACK_YARDS_LOST.last()).item())
        & (COL_SACK_FUMBLES == stat_line.select(COL_SACK_FUMBLES.last()).item())
        & (
            COL_SACK_FUMBLES_LOST
            == stat_line.select(COL_SACK_FUMBLES_LOST.last()).item()
        )
    )

    similar_lines = similar_lines.filter(
        ~(
            (COL_TEAM == stat_line.select(COL_TEAM.last()).item())
            & (COL_SEASON == stat_line.select(COL_SEASON.last()).item())
            & (COL_WEEK == stat_line.select(COL_WEEK.last()).item())
        )
    )

    count: int = similar_lines.height

    if count == 0:
        return None

    week: int = similar_lines.select(COL_WEEK.last()).item()
    season: int = similar_lines.select(COL_SEASON.last()).item()

    return {
        "count": count,
        "week": week,
        "season": season,
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
