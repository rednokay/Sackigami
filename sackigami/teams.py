from dataclasses import dataclass
from typing import Any, Optional, Self

import nflreadpy as nfl
import polars as pl

from sackigami.constants import (
    COL,
    DATA_OF_INTEREST,
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


@dataclass
class SackStatLine:
    """A single teams stat line with the relevant data."""

    gameday: GameDay
    """Gameday the a similar stat line occured the last time."""

    team: str
    """Team of interest."""

    opponent_team: str
    """Opponent team."""

    suffered: int
    """Amount sacks suffered."""

    yards_lost: int
    """Yards lost on sacks."""

    fumbles: int
    """Amount strip sacks/fumbles caused by sacks."""

    fumbles_lost: int
    """Fumbles lost/turnovers caused by strip sacks."""

    def as_dict(self) -> dict[str, int | str]:
        return {
            "season": self.gameday.season,
            "week": self.gameday.week,
            "team": self.team,
            "opponent_team": self.opponent_team,
            "sacks_suffered": self.suffered,
            "sack_yards_lost": self.yards_lost,
            "sack_fumbles": self.fumbles,
            "sack_fumbles_lost": self.fumbles_lost,
        }

    @classmethod
    def from_df(cls, stat_line: pl.DataFrame) -> Self:
        """Create SackStatLine from pl.DataFrame.

        Args:
            stat_line (pl.DataFrame): Sack stat line as dataframe.

        Returns:
            Self: The SackStatLine.
        """
        return cls(
            gameday=GameDay(
                season=stat_line.select(COL.season.last()).item(),
                week=stat_line.select(COL.week.last()).item(),
            ),
            team=stat_line.select(COL.team.last()).item(),
            opponent_team=stat_line.select(COL.opponent_team.last()).item(),
            suffered=stat_line.select(COL.sacks_suffered.last()).item(),
            yards_lost=stat_line.select(COL.sack_yards_lost.last()).item(),
            fumbles=stat_line.select(COL.sack_fumbles.last()).item(),
            fumbles_lost=stat_line.select(COL.sack_fumbles_lost.last()).item(),
        )

    @classmethod
    def from_dict(cls, stat_line: dict[str, int | str]) -> Self:
        """Create SackStatLine from dict.

        Args:
            stat_line (dict[str, int | str]): Sack stat line as dict.

        Returns:
            Self: The SackStatLine.
        """
        return cls(
            gameday=GameDay(
                season=int(stat_line["season"]), week=int(stat_line["week"])
            ),
            team=str(stat_line["team"]),
            opponent_team=str(stat_line["opponent_team"]),
            suffered=int(stat_line["sacks_suffered"]),
            yards_lost=int(stat_line["sack_yards_lost"]),
            fumbles=int(stat_line["sack_fumbles"]),
            fumbles_lost=int(stat_line["sack_fumbles_lost"]),
        )


def retrieve_complete_team_stats() -> Any | pl.DataFrame:
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
    complete_team_stats: pl.DataFrame, sack_stat_line: SackStatLine
) -> Optional[SimilarStatLines]:
    """Finds the amount of and the last time a completely similar stat line occured.

    Args:
        complete_team_stats (pl.DataFrame): Complete team stats.
        sack_stat_line (SackStatLine): The stat line to look for.

    Returns:
        Optional[SimilarStatLines]: The similar stat line or None of none found.
    """

    similar_lines: pl.DataFrame = complete_team_stats.filter(
        (COL.sacks_suffered == sack_stat_line.suffered)
        & (COL.sack_yards_lost == sack_stat_line.yards_lost)
        & (COL.sack_fumbles == sack_stat_line.fumbles)
        & (COL.sack_fumbles_lost == sack_stat_line.fumbles_lost)
    )

    similar_lines = similar_lines.filter(
        ~(
            (COL.team == sack_stat_line.team)
            & (COL.season == sack_stat_line.gameday.season)
            & (COL.week == sack_stat_line.gameday.week)
        )
    )

    count: int = similar_lines.height

    if count == 0:
        return None

    last_week: int = similar_lines.select(COL.week.last()).item()
    last_season: int = similar_lines.select(COL.season.last()).item()

    return SimilarStatLines(GameDay(last_season, last_week), count)
