import json
import random
import time
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
import x
from constants import STAT_THRESHOLDS, TEAMS, col
from fetch import (
    find_similar_stat_lines,
    parse_sack_data,
    retrieve_complete_team_stats,
    retrieve_week,
    parse_last_gameday,
)

# TODO: Reduce save file size by only storing identifyind data
# TODO: Delete/clear save file when week is over
# TODO: Wrapped at the end of the season
# TODO: What happens to count if several same results on the same day
# TODO: Handle negative https reponse

SAVE_PATH: Path = Path("posted.json")
"""Default save path for JSON game data."""

OFFLINE_TEST: bool = True or not Path(".env").exits()
"""Flag to disable and enable posting X and using the X API.

For True the X API will not be called.
"""

POST_TIMEOUT = 45
"""Base timeout between seperate X posts."""


def save_game_to_json(game: dict[str, int | str], path: Path = SAVE_PATH) -> None:
    """Saves game data to a file in JSON format.

    Args:
        game (dict[str, int  |  str]): Dict that contains game data.
        path (Path, optional): Path where to save the JSON data. Defaults to SAVE_PATH.
    """
    exiting_games: list[dict[str, int | str]] = load_game_from_json(path)
    exiting_games.append(game)

    path.write_text(json.dumps(exiting_games, indent=4))


def load_game_from_json(path: Path = SAVE_PATH) -> list[dict[str, int | str]]:
    """Load game data from a JSON file.

    Args:
        path (Path, optional): Path to the saved gama data JSON file. Defaults to SAVE_PATH.

    Returns:
        list[dict[str, int | str]]: List with all saved game data.
    """
    if path.exists():
        return json.loads(path.read_text())
    else:
        return []


def plural_s(word: str, num: int | float) -> str:
    """Pluralize words that are pularilzed with an appending 's' when needing the plural.

    Args:
        word (str): Word to pluralize in singular.
        num (int): Number on which to dependent the form on.

    Returns:
        str: Singular or plural of word.
    """
    return word + "s" if num != 1 else word


def create_string(game: dict[str, int | str], similar: Optional[dict[str, int]]) -> str:
    """Creates a string which is to be posted on stdout and X.

    Args:
        game (dict[str, int  |  str]): Dict that contains game data.
        similar (Optional[dict[str, int]]): Dict that contains data how often the same game stats happened before. None if never.

    Returns:
        str: The string which is to be posted.
    """
    output: list[str] = []
    team: str = TEAMS[game["team"]]
    opponent_team: str = TEAMS[game["opponent_team"]]
    sacks_suffered: int = int(game["sacks_suffered"])
    sack_yards_lost: int = int(game["sack_yards_lost"])
    sack_fumbles: int = int(game["sack_fumbles"])
    sack_fumbles_lost: int = int(game["sack_fumbles_lost"])

    output.append(
        f"The {team} suffered {sacks_suffered} {plural_s("sack", sacks_suffered)} in their game against the {opponent_team}. This lead to a total of {abs(sack_yards_lost)} {plural_s("yard", sack_yards_lost)} lost."
    )
    if sack_fumbles == 1:
        output.append(
            f"That sack was a strip-sack, resulting in {sack_fumbles_lost} {plural_s("turnover", sack_fumbles_lost)}."
        )
    else:
        output.append(
            f"{sack_fumbles} of those sacks were strip-sacks, resulting in {sack_fumbles_lost} {plural_s("turnover", sack_fumbles_lost)}."
        )

    if similar is None:
        output.insert(0, "Sackigami!\n")
        output.append("\nThis has never happened before.")
    else:
        output.insert(0, "No Sackigami!\n")
        output.append(
            f"\nThis has happened {similar["count"]} {plural_s("time", similar["count"])} before. Most recently in week {similar["week"]} of the {similar["season"]} season."
        )

    return "\n".join(output)


def random_delay(
    base: int = POST_TIMEOUT, variance: int = int(POST_TIMEOUT * 0.3)
) -> float:
    """Creates a random delay time distributed around a given base.

    Args:
        base (int, optional): Base value to distribute around. Defaults to POST_TIMEOUT.
        variance (int, optional): Variance around base value. Defaults to int(POST_TIMEOUT * 0.3).

    Returns:
        float: Delay time.
    """
    delay: float = random.uniform(base - variance, base + variance)
    return max(delay, POST_TIMEOUT * 0.45)


def apply_delay():
    """Apply a random delay."""
    delay = random_delay()
    print(f"Sleeping for {delay} seconds ...")
    time.sleep(delay)


def post(
    game: dict[str, int | str],
    similar: Optional[dict[str, int]],
    path: Path = SAVE_PATH,
) -> None:
    """Posts a game to stdout and X.

    Args:
        game (dict[str, int  |  str]): Dict that contains game data.
        similar (Optional[dict[str, int]]): Dict that contains data how often the same game stats happened before. None if never.
        path (Path, optional): Path where to save games of completed posts. Defaults to SAVE_PATH.
    """
    output: str = create_string(game, similar)

    print(output)

    if not OFFLINE_TEST:
        x.post(output)

    save_game_to_json(game, path)

    if not OFFLINE_TEST:
        apply_delay()


def has_been_posted(game: dict[str, int | str], path: Path = SAVE_PATH) -> bool:
    """Checks whether a game has already been posted.

    Args:
        game (dict[str, int  |  str]): Dict that contains game data.
        path (Path, optional): Path where to look for saved games in JSON format. Defaults to SAVE_PATH.

    Returns:
        bool: True if game has been posted, False if not.
    """
    exiting_games: list[dict[str, int | str]] = load_game_from_json(path)
    return game in exiting_games


def worth_posting(
    game: dict[str, int | str], similar: Optional[dict[str, int]]
) -> bool:
    """Checks whether a game is worth posting.

    The game is compared against certain thresholds or milestones in order to
    decide on whether it is worth posting.

    Args:
        game (dict[str, int  |  str]): Dict that contains the game data.
        similar (Optional[dict[str, int]]): Dict that contains data how often the same game stats happened before. None if never.

    Returns:
        bool: True if the game is worth, False if not.
    """

    if has_been_posted(game):
        return False

    if similar is None:
        return True

    if similar["count"] <= 4:
        return True

    if similar["season"] <= date.today().year - 15:
        return True

    if (
        game["sacks_suffered"] >= STAT_THRESHOLDS["sacks_suffered"]
        and game["sack_yards_lost"] == 0
    ):
        return True

    if (
        (game["sacks_suffered"] >= STAT_THRESHOLDS["sacks_suffered"])
        or (
            abs(int(game["sack_yards_lost"])) >= abs(STAT_THRESHOLDS["sack_yards_lost"])
        )
        or (game["sack_fumbles"] >= STAT_THRESHOLDS["sack_fumbles"])
        or (game["sack_fumbles_lost"] >= STAT_THRESHOLDS["sack_fumbles_lost"])
    ):
        return True

    return False


def loop_over_week(week: pl.DataFrame, complete_team_stats: pl.DataFrame) -> None:
    """Iterates over a game day, parses the data and post Sackigami! data.

    Args:
        week (pl.DataFrame): Game stats of the week,
        complete_team_stats (pl.DataFrame): All stats.
    """
    week_sack_data: pl.DataFrame = parse_sack_data(week)
    for game in week_sack_data.iter_rows(named=True):
        sim: Optional[dict[str, int]] = find_similar_stat_lines(
            complete_team_stats, game
        )
        print("--------------")
        if sim is None:
            post(game, sim)
        else:
            if worth_posting(game, sim):
                post(game, sim)

    if OFFLINE_TEST:
        Path(SAVE_PATH).unlink(missing_ok=True)


def no_sack_average(complete_team_stats: pl.DataFrame) -> float:
    game_day: dict[str, int] = parse_last_gameday(complete_team_stats)

    this_season_no_sacks: pl.DataFrame = complete_team_stats.filter(
        (col.season == game_day["season"]) & (col.sacks_suffered == 0)
    )
    amount: int = this_season_no_sacks.height

    return amount / game_day["week"]


def loop_over_no_sacks(week: pl.DataFrame, complete_team_stats: pl.DataFrame) -> None:
    """Iterates over a game day, parses the data and post teams that did not surrender a sack.

    Args:
        week (pl.DataFrame): Game stats of the week,
        complete_team_stats (pl.DataFrame): All stats.
    """
    teams_no_sacks: list[str] = []

    week_sack_data: pl.DataFrame = parse_sack_data(week)
    for game in week_sack_data.iter_rows(named=True):
        if game["sacks_suffered"] == 0:
            teams_no_sacks.append(game["team"])

    output: str = create_string_no_sacks(teams_no_sacks, complete_team_stats)

    print(output)

    if not OFFLINE_TEST:
        x.post(output)

        apply_delay()


def create_string_no_sacks(
    teams_no_sacks: list[str], complete_team_stats: pl.DataFrame
) -> str:
    """Creates a string for teams which did not surrender a sack in a week.

    Args:
        teams_no_sacks (list[str]): List of teams that did not get sacked.
        complete_team_stats (pl.DataFrame): All stats.

    Returns:
        str: String to be posted.
    """
    output: list[str] = []

    output.append(
        "Congratulations! The following teams did not surrender a sack this week:\n"
    )

    for team in teams_no_sacks:
        output.append(TEAMS[team])
    avg: float = no_sack_average(complete_team_stats)
    output.append(
        f"\nThis season, on average {avg:.2f} {plural_s("team", round(avg, 2))} do not surrender a sack per game day."
    )

    return "\n".join(output)
