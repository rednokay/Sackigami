import typer
import polars as pl
from bot import (
    retrieve_complete_team_stats,
    retrieve_week,
    loop_over_week,
    loop_over_no_sacks,
)

APP_KWARGS: dict[str, str | bool] = {
    "suggest_commands": True,
    "no_args_is_help": True,
}
"""Kwargs used for typer constructor."""

app = typer.Typer(**APP_KWARGS)
"""Typer app."""


# TODO: Option to post on X with disabling degbug mode
@app.command()
def gbg() -> None:
    """Runs the game-by-game Sackigami!"""
    print("Getting game data ...")
    complete_stats: pl.DataFrame = retrieve_complete_team_stats()

    print("Parse latest week and filter for relevancy ...")
    last_week: pl.DataFrame = retrieve_week(complete_stats)

    print("Looping over games")
    loop_over_week(last_week, complete_stats)


@app.command()
def nosacks() -> None:
    """Run and post teams that did not get sacked."""

    print("Getting game data ...")
    complete_stats: pl.DataFrame = retrieve_complete_team_stats()

    print("Parse latest week and filter for relevancy ...")
    last_week: pl.DataFrame = retrieve_week(complete_stats)

    print("Looping over games")
    loop_over_no_sacks(last_week, complete_stats)


if __name__ == "__main__":
    app()
