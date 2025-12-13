from typing import Optional

import nflreadpy as nfl
import polars as pl

TEAMS: dict[str, str] = {
    "ATL": "Atlanta Falcons",
    "TB": "Tampa Bay Buccaneers",
    "WAS": "Washington Commanders",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "BAL": "Baltimore Ravens",
}

DATA_OF_INTEREST: tuple[str, ...] = (
    "team",
    "season",
    "week",
    "opponent_team",
    "sacks_suffered",
    "sack_yards_lost",
    "sack_fumbles",
    "sack_fumbles_lost",
)

STAT_THRESHOLDS: dict[str, int] = {
    "sacks_suffered": 5,
    "sack_yards_lost": -25,
    "sack_fumbles": 2,
    "sack_fumbles_lost": 2,
}


def retrieve_complete_team_stats() -> pl.DataFrame:
    return nfl.load_team_stats(seasons=True, summary_level="week")


def retrieve_last_week(complete_team_stats: pl.DataFrame) -> pl.DataFrame:
    last_season: int = complete_team_stats.select(pl.col("season").last()).item()
    last_week: int = complete_team_stats.select(pl.col("week").last()).item()

    return complete_team_stats.filter(
        (pl.col("season") == last_season) & (pl.col("week") == last_week)
    )


def parse_sack_data(weekly_stat_lines: pl.DataFrame) -> pl.DataFrame:
    return weekly_stat_lines.select([data for data in DATA_OF_INTEREST])


def filter_by_threshold(stat_lines: pl.DataFrame) -> pl.DataFrame:
    return stat_lines.filter(
        (pl.col("sacks_suffered") >= STAT_THRESHOLDS["sacks_suffered"])
        | (pl.col("sack_yards_lost") <= STAT_THRESHOLDS["sack_yards_lost"])
        | (pl.col("sack_fumbles") >= STAT_THRESHOLDS["sack_fumbles"])
        | (pl.col("sack_fumbles_lost") >= STAT_THRESHOLDS["sack_fumbles_lost"])
    )


def find_similar_stat_lines(
    complete_team_stats: pl.DataFrame, stat_line: pl.DataFrame
) -> Optional[dict[str, int]]:
    similar_lines: pl.DataFrame = complete_team_stats.filter(
        (
            pl.col("sacks_suffered")
            == stat_line.select(pl.col("sacks_suffered").last()).item()
        )
        & (
            pl.col("sack_yards_lost")
            == stat_line.select(pl.col("sack_yards_lost").last()).item()
        )
        & (
            pl.col("sack_fumbles")
            == stat_line.select(pl.col("sack_fumbles").last()).item()
        )
        & (
            pl.col("sack_fumbles_lost")
            == stat_line.select(pl.col("sack_fumbles_lost").last()).item()
        )
    )

    similar_lines = similar_lines.filter(
        ~(
            (pl.col("team") == stat_line.select(pl.col("team").last()).item())
            & (pl.col("season") == stat_line.select(pl.col("season").last()).item())
            & (pl.col("week") == stat_line.select(pl.col("week").last()).item())
        )
    )

    count: int = similar_lines.height

    if count == 0:
        return None

    week: int = similar_lines.select(pl.col("week").last()).item()
    season: int = similar_lines.select(pl.col("season").last()).item()

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

    prev: Optional[dict[str, int]] = find_similar_stat_lines(complete_team_stats, stat_line)

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
