import polars as pl

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
    "sack_yards_lost": -30,
    "sack_fumbles": 1,
    "sack_fumbles_lost": 1,
}

COL_TEAM: pl.Expr = pl.col("team")
COL_SEASON: pl.Expr = pl.col("season")
COL_WEEK: pl.Expr = pl.col("week")
COL_OPPONENT_TEAM: pl.Expr = pl.col("opponent_team")
COL_SACKS_SUFFERED: pl.Expr = pl.col("sacks_suffered")
COL_SACK_YARDS_LOST: pl.Expr = pl.col("sack_yards_lost")
COL_SACK_FUMBLES: pl.Expr = pl.col("sack_fumbles")
COL_SACK_FUMBLES_LOST: pl.Expr = pl.col("sack_fumbles_lost")
