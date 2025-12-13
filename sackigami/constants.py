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

COL_TEAM: pl.Expr = pl.col("team")
COL_SEASON: pl.Expr = pl.col("season")
COL_WEEK: pl.Expr = pl.col("week")
COL_OPPONENT_TEAM: pl.Expr = pl.col("opponent_team")
COL_SACKS_SUFFERED: pl.Expr = pl.col("sacks_suffered")
COL_SACK_YARDS_LOST: pl.Expr = pl.col("sack_yards_lost")
COL_SACK_FUMBLES: pl.Expr = pl.col("sack_fumbles")
COL_SACK_FUMBLES_LOST: pl.Expr = pl.col("sack_fumbles_lost")
