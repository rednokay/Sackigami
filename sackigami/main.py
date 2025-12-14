from typing import Optional

from fetch import (
    filter_by_threshold,
    find_similar_stat_lines,
    parse_sack_data,
    print_stat_line,
    retrieve_complete_team_stats,
    retrieve_week,
)


def main():
    data = retrieve_complete_team_stats()
    cw = filter_by_threshold(retrieve_week(data))
    sacks = parse_sack_data(cw)

    print(sacks)

    sim: Optional[dict] = find_similar_stat_lines(data, cw)

    print(sim)
    print_stat_line(cw, data)


if __name__ == "__main__":
    main()
