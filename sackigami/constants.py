import polars as pl
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnsOfInterest:
    team: pl.Expr = field(default_factory=lambda: pl.col("team"))
    """Team stats correspond to."""

    season: pl.Expr = field(default_factory=lambda: pl.col("season"))
    """Season or year of the game."""

    week: pl.Expr = field(default_factory=lambda: pl.col("week"))
    """Week or gameday of the game."""

    opponent_team: pl.Expr = field(default_factory=lambda: pl.col("opponent_team"))
    """Opponent of the team."""

    sacks_suffered: pl.Expr = field(default_factory=lambda: pl.col("sacks_suffered"))
    """Sacks surrendered by the team."""

    sack_yards_lost: pl.Expr = field(default_factory=lambda: pl.col("sack_yards_lost"))
    """Yards lost on all sacks combined."""

    sack_fumbles: pl.Expr = field(default_factory=lambda: pl.col("sack_fumbles"))
    """Sacks which ended in fumbles, aka strip-sacks."""

    sack_fumbles_lost: pl.Expr = field(
        default_factory=lambda: pl.col("sack_fumbles_lost")
    )
    """Fumbles lost or turnovers after a strip-sack."""


col: ColumnsOfInterest = ColumnsOfInterest()


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
"""Tuple containg data fields of interest."""

TEAMS: dict[str, str] = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LAC": "Los Angeles Chargers",
    "LA": "Los Angeles Rams",
    "LV": "Las Vegas Raiders",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SEA": "Seattle Seahawks",
    "SF": "San Francisco 49ers",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders",
}
"""Dictionary of all NFL teams. Short as keys long as values."""


STAT_THRESHOLDS: dict[str, int] = {
    "sacks_suffered": 6,
    "sack_yards_lost": -35,
    "sack_fumbles": 2,
    "sack_fumbles_lost": 1,
}
"""Dict containing stat threshold for posting."""
