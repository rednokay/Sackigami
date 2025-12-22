from pathlib import Path
from typing import Optional

import polars as pl
import pytest
from bot import (
    create_string,
    has_been_posted,
    load_game_from_json,
    loop_over_week,
    no_sack_average,
    post,
    save_game_to_json,
    set_correct_path,
    SimilarStatLines,
    GameDay,
)
from fetch import retrieve_weekly_stats
from test_fetch import complete_stats, complete_stats_no_repeats

# TODO: Test worth_posting, loop over week


@pytest.fixture
def game() -> dict[str, int | str]:
    return {
        "season": 2025,
        "week": 16,
        "team": "WAS",
        "opponent_team": "BAL",
        "sacks_suffered": 7,
        "sack_yards_lost": -45,
        "sack_fumbles": 3,
        "sack_fumbles_lost": 2,
    }


@pytest.fixture
def similar_not_none() -> SimilarStatLines:
    return SimilarStatLines(GameDay(2002, 12), 5)


class TestSaveGameToJsonAndLoadGameFromJson:
    def test_no_exiting_games(self, game, tmp_path):
        save_path = tmp_path / "games.json"

        assert not save_path.exists()
        assert not load_game_from_json(None, save_path)

        save_game_to_json(game, None, save_path)

        assert save_path.exists()
        assert [game] == load_game_from_json(None, save_path)

    def test_existing_games(self, game, tmp_path):
        save_path = tmp_path / "games.json"

        save_game_to_json(game, None, save_path)
        save_game_to_json(game, None, save_path)

        assert save_path.exists()
        assert [game, game] == load_game_from_json(None, save_path)


class TestCreateString:
    def test_no_sackigami(self, game, similar_not_none):
        created: str = create_string(game, similar_not_none)

        expected: str = (
            "No Sackigami!\n\n"
            "The Washington Commanders suffered 7 sacks in their game against the Baltimore Ravens. "
            "This led to a total of 45 yards lost.\n"
            "3 of those sacks were strip-sacks, resulting in 2 turnovers.\n\n"
            "This has happened 5 times before. Most recently in week 12 of the 2002 season."
        )

        assert created == expected

    def test_to_sackigami_singular(self, game, similar_not_none):
        similar_single: SimilarStatLines = similar_not_none
        similar_single.count = 1

        game_single: dict[str, int | str] = game
        for key, val in game_single.items():
            match val:
                case int():
                    game_single[key] = 1

        created: str = create_string(game_single, similar_single)

        expected: str = (
            "No Sackigami!\n\n"
            "The Washington Commanders suffered 1 sack in their game against the Baltimore Ravens. "
            "This led to a total of 1 yard lost.\n"
            "That sack was a strip-sack, resulting in 1 turnover.\n\n"
            "This has happened 1 time before. Most recently in week 12 of the 2002 season."
        )

        assert created == expected

    def test_sackigami(self, game):
        created: str = create_string(game, None)

        expected: str = (
            "Sackigami!\n\n"
            "The Washington Commanders suffered 7 sacks in their game against the Baltimore Ravens. "
            "This led to a total of 45 yards lost.\n"
            "3 of those sacks were strip-sacks, resulting in 2 turnovers.\n\n"
            "This has never happened before."
        )

        assert created == expected


class TestHasBeenPosted:
    def test_has_not_been_posted(self, game, tmp_path):
        save_path = tmp_path / "games.json"
        assert not has_been_posted(game, None, save_path)

    def test_has_been_posted(self, game, tmp_path):
        save_path = tmp_path / "games.json"
        post(game, None, None, save_path)

        assert save_path.exists()
        assert has_been_posted(game, None, save_path)


# TODO: Add test for no sackigami
class TestLoopOverWeek:
    def test_sackigami(self, capsys, complete_stats_no_repeats):
        last_week: pl.DataFrame = retrieve_weekly_stats(complete_stats_no_repeats)
        loop_over_week(last_week, complete_stats_no_repeats)

        captured: pytest.capture.CapturedResults = capsys.readouterr()

        assert "Sackigami!" in captured.out
        assert "No Sackigami!" not in captured.out


def test_no_sack_average():
    length: int = 17
    seasons: list[int] = [2025 for _ in range(length * 2)]
    week: list[int] = [int(i / 2) for i in range(2, length * 2 + 2)]
    sacks: list[int] = [0 for _ in range(length * 2)]

    complete_teams_stats = pl.DataFrame(
        {
            "season": seasons,
            "week": week,
            "sacks_suffered": sacks,
        }
    )

    res: float = no_sack_average(complete_teams_stats)
    expected: float = 2.0

    assert res == expected


class TestSetCorrectPath:
    def test_no_path_given(self, tmp_path):
        path: Optional[Path] = None
        fallback = tmp_path / "games.json"

        assert set_correct_path(path, fallback) == fallback

    # TODO: Some how test with OFFLINE_TEST = True
    def test_path_given(self, tmp_path):
        path: Optional[Path] = tmp_path / "games.json"

        fallback = tmp_path / "fallback.json"

        # This is due to OFFLINE_TEST
        assert set_correct_path(path, fallback) == fallback
