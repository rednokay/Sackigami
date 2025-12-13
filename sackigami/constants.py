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
