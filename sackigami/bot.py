from fetch import (
    retrieve_complete_team_stats,
    retrieve_week,
    filter_by_threshold,
    parse_sack_data,
    find_similar_stat_lines,
)
from constants import TEAMS
import polars as pl
import json
import x
from typing import Optional
from pathlib import Path

# TODO: At the end of a week, post all 0 sack teams
# TODO: Add sacks but no yardage lost?
# TODO: Rare stats lines
# TODO: Reduce save file size by only storing identifyind data
# TODO: Delete/clear save file when week is over
# TODO: Wrapped at the end of the season

OFFLINE_TEST: bool = True

SAVE_PATH: Path = Path("posted.json")


def save_game_to_json(game: dict[str, int | str], path: Path = SAVE_PATH) -> None:
    exiting_games: list[dict[str, int | str]] = load_game_from_json(path)
    exiting_games.append(game)

    path.write_text(json.dumps(exiting_games, indent=4))


def load_game_from_json(path: Path = SAVE_PATH) -> list[dict[str, int | str]]:
    if path.exists():
        return json.loads(path.read_text())
    else:
        return []


def plural_s(word: str, num: int) -> str:
    return word + "s" if num != 1 else word


def create_string(game: dict[str, int | str], similar: Optional[dict[str, int]]) -> str:
    output: list[str] = []
    team: str = TEAMS[game["team"]]
    opponent_team: str = TEAMS[game["opponent_team"]]
    sacks_suffered: int = int(game["sacks_suffered"])
    sack_yards_lost: int = int(game["sack_yards_lost"])
    sack_fumbles: int = int(game["sack_fumbles"])
    sack_fumbles_lost: int = int(game["sack_fumbles_lost"])

    output.append(
        f"The {team} suffered {sacks_suffered} sacks in their game against the {opponent_team}. This lead to a total of {abs(sack_yards_lost)} yards lost."
    )
    output.append(
        f"{sack_fumbles} of those sacks were strip-sacks, resulting in {sack_fumbles_lost} {plural_s("turnover", sack_fumbles_lost)}."
    )

    if similar is None:
        output.insert(0, "Sackigami!\n")
        output.append("\nThis has never happened before.")
    else:
        output.insert(0, "No Sackigami!\n")
        plural_time: str = "times" if similar["count"] != 1 else "time"
        output.append(
            f"\nThis has happened {similar["count"]} {plural_s("time", similar["count"])} before. Most recently in week {similar["week"]} of the {similar["season"]} season."
        )

    return "\n".join(output)


def post(
    game: dict[str, int | str],
    similar: Optional[dict[str, int]],
    path: Path = SAVE_PATH,
) -> None:
    save_game_to_json(game, path)
    output: str = create_string(game, similar)

    print(output)

    if not OFFLINE_TEST:
        x.post(output)


def has_been_posted(game: dict[str, int | str], path: Path = SAVE_PATH) -> bool:
    exiting_games: list[dict[str, int | str]] = load_game_from_json(path)
    return game in exiting_games


def worth_posting(game: dict[str, int | str], similar: dict[str, int]) -> bool:
    if has_been_posted(game):
        return False
    if similar["count"] == 0:
        return True
    return False


def loop_over_week(week: pl.DataFrame, complete_team_stats: pl.DataFrame) -> None:
    week_sack_data = parse_sack_data(week)
    for game in week_sack_data.iter_rows(named=True):
        sim: Optional[dict[str, int]] = find_similar_stat_lines(
            complete_team_stats, game
        )
        if sim is None:
            print(f"Sackigami! {game["team"]} vs. {game["opponent_team"]}")
            if not OFFLINE_TEST:
                post(game, sim)
        else:
            print(f"No sackigami! {game["team"]} vs. {game["opponent_team"]}")
            if worth_posting(game, sim):
                print("Game is worth posting ...")
                if not OFFLINE_TEST:
                    post(game, sim)


def main() -> None:
    print("Getting game data ...")
    complete_stats: pl.DataFrame = retrieve_complete_team_stats()

    print("Parse latest week and filter for relevancy ...")
    last_week: pl.DataFrame = retrieve_week(complete_stats)

    print("Looping over games")
    loop_over_week(last_week, complete_stats)


if __name__ == "__main__":
    main()
