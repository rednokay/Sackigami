import random

import polars as pl
import pytest

from fetch import (
    find_similar_stat_lines,
    retrieve_weekly_stats,
    GameDay,
    SimilarStatLines,
)
from constants import TEAMS


# TODO: What if the randoms actually generate equal stat lines?
@pytest.fixture
def complete_stats() -> pl.DataFrame:
    season: list[int] = [
        1999,
        2000,
        2013,
        2013,
        2025,
        2025,
        2025,
    ]
    week: list[int] = [
        5,
        random.randint(1, 18),
        random.randint(1, 10),
        random.randint(10, 18),
        13,
        16,
        16,
    ]

    teams_short: list[str] = list(TEAMS.keys())
    team: list[str] = [
        "WAS",
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        "WAS",
        "BUF",
        "WAS",
    ]
    opponent_team: list[str] = [
        "BAL",
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        "CAR",
        "BAL",
    ]
    sacks_suffered: list[int] = [
        7,
        2,
        2,
        6,
        7,
        7,
        7,
    ]
    sack_yards_lost: list[int] = [
        -45,
        -12,
        -13,
        -20,
        -45,
        -45,
        -45,
    ]
    sack_fumbles: list[int] = [
        3,
        random.randint(0, 3),
        random.randint(0, 3),
        random.randint(0, 3),
        3,
        3,
        3,
    ]
    sack_fumbles_lost: list[int] = [
        2,
        random.randint(0, sack_fumbles[1] + 1),
        random.randint(0, sack_fumbles[2] + 1),
        random.randint(0, sack_fumbles[3] + 1),
        2,
        2,
        2,
    ]

    return pl.DataFrame(
        {
            "season": season,
            "week": week,
            "team": team,
            "opponent_team": opponent_team,
            "sacks_suffered": sacks_suffered,
            "sack_yards_lost": sack_yards_lost,
            "sack_fumbles": sack_fumbles,
            "sack_fumbles_lost": sack_fumbles_lost,
        }
    )


@pytest.fixture
def complete_stats_no_repeats() -> pl.DataFrame:
    season: list[int] = [
        1999,
        2000,
        2013,
        2013,
        2025,
        2025,
        2025,
    ]
    week: list[int] = [
        random.randint(1, 18),
        random.randint(1, 18),
        random.randint(1, 10),
        random.randint(10, 18),
        13,
        16,
        16,
    ]

    teams_short: list[str] = list(TEAMS.keys())
    team: list[str] = [
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        "WAS",
        "BUF",
        "WAS",
    ]
    opponent_team: list[str] = [
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        random.choices(teams_short)[0],
        "CAR",
        "BAL",
    ]
    sacks_suffered: list[int] = [
        7,
        2,
        2,
        6,
        8,
        6,
        7,
    ]
    sack_yards_lost: list[int] = [
        -45,
        -12,
        -13,
        -20,
        -45,
        -34,
        -5,
    ]
    sack_fumbles: list[int] = [
        1,
        random.randint(0, 3),
        random.randint(0, 3),
        random.randint(0, 3),
        2,
        3,
        2,
    ]
    sack_fumbles_lost: list[int] = [
        1,
        random.randint(0, sack_fumbles[1] + 1),
        random.randint(0, sack_fumbles[2] + 1),
        random.randint(0, sack_fumbles[3] + 1),
        0,
        0,
        1,
    ]

    return pl.DataFrame(
        {
            "season": season,
            "week": week,
            "team": team,
            "opponent_team": opponent_team,
            "sacks_suffered": sacks_suffered,
            "sack_yards_lost": sack_yards_lost,
            "sack_fumbles": sack_fumbles,
            "sack_fumbles_lost": sack_fumbles_lost,
        }
    )


class TestRetrieveWeeklyStats:
    def test_retrieve_weekly_stats_no_gameday_given(self, complete_stats):
        last_week = retrieve_weekly_stats(complete_stats)

        expected = pl.DataFrame(
            {
                "season": [2025, 2025],
                "week": [16, 16],
                "team": ["BUF", "WAS"],
                "opponent_team": ["CAR", "BAL"],
                "sacks_suffered": [7, 7],
                "sack_yards_lost": [-45, -45],
                "sack_fumbles": [3, 3],
                "sack_fumbles_lost": [2, 2],
            }
        )

        assert last_week.to_dicts() == expected.to_dicts()

    def test_retrieve_weekly_stats_with_gameday(self, complete_stats):
        gameday = GameDay(season=1999, week=5)

        week = retrieve_weekly_stats(complete_stats, gameday)

        expected = pl.DataFrame(
            {
                "season": [1999],
                "week": [5],
                "team": ["WAS"],
                "opponent_team": ["BAL"],
                "sacks_suffered": [7],
                "sack_yards_lost": [-45],
                "sack_fumbles": [3],
                "sack_fumbles_lost": [2],
            }
        )

        assert week.to_dicts() == expected.to_dicts()


class TestFindSimilarStatLines:
    def test_existing_similar_stat_linse(self, complete_stats):
        relevant_last_week = pl.DataFrame(
            {
                "season": 2025,
                "week": 16,
                "team": "WAS",
                "sacks_suffered": 7,
                "sack_yards_lost": -45,
                "sack_fumbles": 3,
                "sack_fumbles_lost": 2,
            }
        )

        sim = find_similar_stat_lines(complete_stats, relevant_last_week)

        assert sim is not None
        assert sim == SimilarStatLines(GameDay(season=2025, week=16), 3)

    def test_no_exiting_similar_stat_line(self, complete_stats_no_repeats):
        relevant_last_week = pl.DataFrame(
            {
                "season": 2025,
                "week": 16,
                "team": "WAS",
                "sacks_suffered": 7,
                "sack_yards_lost": -45,
                "sack_fumbles": 3,
                "sack_fumbles_lost": 2,
            }
        )

        sim = find_similar_stat_lines(complete_stats_no_repeats, relevant_last_week)

        assert sim is None

    def test_existing_similar_stat_linse_dcit(self, complete_stats):
        relevant_last_week = {
            "season": 2025,
            "week": 16,
            "team": "WAS",
            "sacks_suffered": 7,
            "sack_yards_lost": -45,
            "sack_fumbles": 3,
            "sack_fumbles_lost": 2,
        }

        sim = find_similar_stat_lines(complete_stats, relevant_last_week)

        assert sim is not None
        assert sim == SimilarStatLines(GameDay(season=2025, week=16), 3)

    def test_no_exiting_similar_stat_line_dict(self, complete_stats_no_repeats):
        relevant_last_week = {
            "season": 2025,
            "week": 16,
            "team": "WAS",
            "sacks_suffered": 7,
            "sack_yards_lost": -45,
            "sack_fumbles": 3,
            "sack_fumbles_lost": 2,
        }

        sim = find_similar_stat_lines(complete_stats_no_repeats, relevant_last_week)

        assert sim is None
