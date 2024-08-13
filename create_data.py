"""Create data structure from the output folder"""

# disable=C0114
# disable=C0301

import os
import json
import pandas as pd

_ = """
directory structure:

per years, per matches, per teams, per players

output/EPL
└── $year
    ├── EPL_$year.csv
    ├── EPL_$year_extended.csv
    ├── matches
    │   ├── $match_id
    │   ├── $match_id-TeamHome-TeamAway_rosters.csv
    │   └── $match_id-TeamHome-TeamAway_shot.csv
    └── teams
        └── $team_id
            └── $player_name
                ├── $player_id_$player_name_groups.json
                ├── $player_id_$player_name_matches.csv
                └── $player_id_$player_name_shots.csv

"""


def load_match_rosters(
    match_id: str, _league_folder: str, _year: str, _league_data_ext: pd.DataFrame
) -> pd.DataFrame:
    """Load the match rosters data

    Args:
        match_id (str): The match ID
        _league_folder (str): The league folder
        _year (str): The year (Season start year)
        _league_data_ext (pd.DataFrame): The league data extended

    Returns:
        Dict: The match rosters data with keys:
            dict_keys(['rosters', 'rosters_summary'])
                'rosters': pd.DataFrame: The match rosters data with keys:
                    Index(['Team', 'ID', 'Goals', 'Own Goals', 'Shots', 'xG', 'Time', 'Player ID', 'Team ID', 'Position', 'Player', 'Home/Away', 'Yellow Card', 'Red Card', 'Roster In', 'Roster Out', 'Key Passes', 'Assists', 'xA', 'xGChain', 'xGBuildup', 'Position Order', 'Match ID', 'Home Team', 'Away Team', 'Season'], dtype='object')
                'rosters_summary': pd.DataFrame: The match rosters summary data with keys:
                    Index(['Home Goals', 'Away Goals', 'Home Own Goals', 'Away Own Goals', 'Home Shots', 'Away Shots', 'Home xG', 'Away xG', 'Home Time', 'Away Time', 'Home Yellow Card', 'Away Yellow Card', 'Home Red Card', 'Away Red Card', 'Home Key Passes', 'Away Key Passes', 'Home Assists', 'Away Assists', 'Home xA', 'Away xA', 'Home xGChain', 'Away xGChain', 'Home xGBuildup', 'Away xGBuildup'], dtype='object')
    """
    rosters = pd.read_csv(
        os.path.join(
            _league_folder,
            str(_year),
            "matches",
            str(match_id),
            f"{match_id}-{_league_data_ext[_league_data_ext['Match ID'] == match_id]['Home'].values[0]}-{_league_data_ext[_league_data_ext['Match ID'] == match_id]['Away'].values[0]}_rosters.csv",
        )
    ).drop_duplicates()

    rosters["Season"] = _year
    rosters["Match ID"] = match_id
    rosters["League"] = _league_folder.split("/")[-1]

    rosters_summary = rosters.groupby("Team").agg(
        {
            "Goals": "sum",
            "Own Goals": "sum",
            "Shots": "sum",
            "xG": "sum",
            "Time": "sum",
            "Yellow Card": "sum",
            "Red Card": "sum",
            "Key Passes": "sum",
            "Assists": "sum",
            "xA": "sum",
            "xGChain": "sum",
            "xGBuildup": "sum",
        }
    )

    home_rosters_summary = rosters_summary.loc["a"]
    away_rosters_summary = rosters_summary.loc["h"]

    rosters_summary_w_data_ext_keys = pd.DataFrame(
        {
            **{
                f"Home {col_name}": [home_rosters_summary.loc[col_name]]
                for col_name in home_rosters_summary.index
            },
            **{
                f"Away {col_name}": [away_rosters_summary.loc[col_name]]
                for col_name in away_rosters_summary.index
            },
            **{
                "Season": [_year],
                "Match ID": [match_id],
                "League": [_league_folder.split("/")[-1]],
            },
        }
    )

    return {
        "rosters": rosters,
        "rosters_summary": rosters_summary_w_data_ext_keys,
    }


def load_match_shots(
    match_id: str,
    _league_folder: str,
    _year: str,
    _league_data_ext: pd.DataFrame,
) -> pd.DataFrame:
    """Load the match shots data

    Args:
        match_id (str): The match ID
        _league_folder (str): The league folder
        _year (str): The year (Season start year)
        _league_data_ext (pd.DataFrame): The league data extended

    Returns:
        pd.DataFrame: The match shots data with keys:
            Index(['Team', 'Minute', 'Result', 'X', 'Y', 'xG', 'Player', 'Home/Away',  'Player ID', 'Situation', 'Season', 'Shot Type', 'Match ID',  'Home Team', 'Away Team', 'Home Goals', 'Away Goals', 'Date', 'Player Assisted', 'Last Action'], dtype='object')
    """

    return pd.read_csv(
        os.path.join(
            _league_folder,
            str(_year),
            "matches",
            str(match_id),
            f"{match_id}-{_league_data_ext[_league_data_ext['Match ID'] == match_id]['Home'].values[0]}-{_league_data_ext[_league_data_ext['Match ID'] == match_id]['Away'].values[0]}_shots.csv",
        )
    ).drop_duplicates()


def load_team_statistics(
    _team_name: str,
    _league_folder: str,
    _year: str,
) -> dict:
    """Load the team statistics data

    Args:
        _team_name (str): The team name
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        dict: The team statistics data with keys:
            dict_keys(['situation', 'formation', 'gameState', 'timing', 'shotZone', 'attackSpeed', 'result'])

        TODO: Proper schema for the team statistics data
    """

    return json.load(
        open(
            os.path.join(
                _league_folder,
                str(_year),
                "teams",
                str(_team_name),
                f"{_team_name}_statistics.json",
            ),
            "r",
            encoding="utf-8",
        )
    )


def load_player_id(
    _team_name: str,
    _player_name: str,
    _league_folder: str,
    _year: str,
) -> str:
    """Load the player ID

    Args:
        _team_name (str): The team name
        _player_name (_type_): The player name
        _year (str): The year (Season start year)

    Returns:
        str: The player ID
    """
    return os.listdir(
        os.path.join(
            _league_folder,
            str(_year),
            "teams",
            str(_team_name),
        )
    )[0].split("_")[0]


def load_player_matches(
    _team_name: str,
    _player_name: str,
    _player_id: str,
    _league_folder: str,
    _year: str,
) -> pd.DataFrame:
    """Load the player matches data

    Args:
        _team_name (str): The team name
        _player_name (_type_): The player name

    Returns:
        pd.DataFrame: The player matches data with keys:
            Index(['Goals', 'Shots', 'xG', 'Time', 'Position', 'Home Team', 'Away Team',  'Home Goals', 'Away Goals', 'Date', 'ID', 'Season', 'Roster ID', 'xA', 'Assists', 'Key Passes', 'NPG', 'NPxG', 'xGChain', 'xGBuildup'], dtype='object')
    """
    # print(type(_team_name), "team_name", _team_name)
    # print(type(_player_name), "player_name", _player_name)
    # print(_league_folder, _year, _team_name, _player_name)

    df = pd.read_csv(
        os.path.join(
            _league_folder,
            str(_year),
            "teams",
            str(_team_name),
            f"{_player_id}-{_player_name}",
            list(
                filter(
                    lambda x: x.endswith("_matches.csv"),
                    os.listdir(
                        os.path.join(
                            _league_folder,
                            str(_year),
                            "teams",
                            str(_team_name),
                            f"{_player_id}-{_player_name}",
                        )
                    ),
                )
            )[0],
        )
    ).drop_duplicates()

    # Agregar las columnas _league_folder y _year a cada fila del DataFrame
    df["League"] = _league_folder.split("/")[-1]
    df["Season"] = _year

    return df


def load_player_shots(
    _team_name: str,
    _player_name: str,
    _player_id: str,
    _league_folder: str,
    _year: str,
) -> pd.DataFrame:
    """Load the player shots data

    Args:
        _team_name (str): The team name
        _player_name (str): The player name
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        pd.DataFrame: The player shots data with keys:
            Index(['Minute', 'Result', 'X', 'Y', 'xG', 'Situation', 'Season', 'Shot Type', 'Match ID', 'Home Team', 'Away Team', 'Home Goals', 'Away Goals', 'Date', 'Player Assisted', 'Last Action'], dtype='object')
    """
    return pd.read_csv(
        # read the *_shots.csv file in the folder
        os.path.join(
            _league_folder,
            str(_year),
            "teams",
            str(_team_name),
            f"{_player_id}-{_player_name}",
            list(
                filter(
                    lambda x: x.endswith("_shots.csv"),
                    os.listdir(
                        os.path.join(
                            _league_folder,
                            str(_year),
                            "teams",
                            str(_team_name),
                            f"{_player_id}-{_player_name}",
                        )
                    ),
                )
            )[0],
        )
    ).drop_duplicates()


def load_teams_groups_json(
    _team_name: str,
    _player_name: str,
    _player_id: str,
    _league_folder: str,
    _year: str,
) -> dict:
    """Load the teams groups json data

    Args:
        _team_name (str): The team name
        _player_name (str): The player name
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        dict: The teams groups json data with keys:
            dict_keys(['shotZones', 'shotTypes', 'positions', 'situations', 'gameStates', 'timing', 'result'])

        TODO: Proper schema for the teams groups json data
    """
    return json.load(
        # read the *_groups.json file in the folder
        open(
            os.path.join(
                _league_folder,
                str(_year),
                "teams",
                str(_team_name),
                f"{_player_id}-{_player_name}",
                list(
                    filter(
                        lambda x: x.endswith("_groups.json"),
                        os.listdir(
                            os.path.join(
                                _league_folder,
                                str(_year),
                                "teams",
                                str(_team_name),
                                f"{_player_id}-{_player_name}",
                            )
                        ),
                    )
                )[0],
            ),
            "r",
            encoding="utf-8",
        )
    )


def player_data(
    _team_name: str,
    _player_name: str,
    _player_id: str,
    _league_folder: str,
    _year: str,
) -> dict:
    """Load the player data

    Args:
        _team_name (str): The team name
        _player_name (str): The player name
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        dict: The player data with keys:
            dict_keys(['player_name', 'player_id', 'matches', 'shots', 'groups'])
    """
    # print(_player_name, "player_name")
    # print(_player_id, "player_id")

    return {
        "player_name": _player_name,
        "player_id": _player_id,
        "matches": load_player_matches(
            _team_name, _player_name, _player_id, _league_folder, _year
        ),
        "shots": load_player_shots(
            _team_name, _player_name, _player_id, _league_folder, _year
        ),
        "groups": load_teams_groups_json(
            _team_name, _player_name, _player_id, _league_folder, _year
        ),
    }


def team_data(
    _team_name: str,
    _league_folder: str,
    _year: str,
) -> dict:
    """Load the team data

    Args:
        _team_name (str): The team name
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        dict: The team data with keys:
            dict_keys(['team_name', 'statistics', 'players'])
    """
    # print(
    #     os.listdir(os.path.join(_league_folder, str(_year), "teams", str(_team_name)))
    # )

    # print(
    #     list(
    #         map(
    #             lambda x: (x.split("-")),
    #             filter(
    #                 lambda x: not (x.endswith(".csv") or x.endswith(".json"))
    #                 or len(x.split("-")) != 2
    #                 and any(
    #                     [
    #                         not os.path.isdir(
    #                             os.path.join(
    #                                 _league_folder,
    #                                 str(_year),
    #                                 "teams",
    #                                 str(_team_name),
    #                                 p,
    #                             )
    #                         )
    #                         for p in x.split("-")
    #                     ]
    #                 ),
    #                 os.listdir(
    #                     os.path.join(
    #                         _league_folder, str(_year), "teams", str(_team_name)
    #                     )
    #                 ),
    #             ),
    #         )
    #     )
    # )

    # print(type(_team_name), "team_name")
    # print(type(_league_folder), "league_folder")
    # print(type(_year), "year")

    player_id_name = list(
        map(
            lambda x: (x.split("-", 1)),
            filter(
                lambda x: (
                    len(x.split("-", 1)) == 2
                    and not x.endswith(".csv")
                    and not x.endswith(".json")
                    and os.path.isdir(
                        os.path.join(_league_folder, str(_year), "teams", str(_team_name), str(x))
                    )
                ),
                os.listdir(
                    os.path.join(
                        _league_folder,
                        str(_year),
                        "teams",
                        str(_team_name)
                    )
                ),
            ),
        )
    )

    # print(player_id_name)

    return {
        "team_name": _team_name,
        "statistics": load_team_statistics(_team_name, _league_folder, _year),
        "players": [
            player_data(_team_name, player_name, player_id, _league_folder, _year)
            for player_id, player_name in player_id_name
            if os.path.isdir(
                os.path.join(
                    _league_folder,
                    str(_year),
                    "teams",
                    str(_team_name),
                    f"{player_id}-{player_name}",
                )
            ) and len(os.listdir(
                os.path.join(
                    _league_folder,
                    str(_year),
                    "teams",
                    str(_team_name),
                    f"{player_id}-{player_name}",
                ) 
            )) > 0
        ],
    }


def matches_data(
    _league_folder: str,
    _year: str,
    _league_data_ext: pd.DataFrame,
) -> list:
    """Load the matches data

    Args:
        _league_folder (str): The league folder
        _year (str): The year (Season start year)
        _league_data_ext (pd.DataFrame): The league data extended

    Returns:
        list: The matches data with keys:
            list_keys(['match_id', 'rosters', 'shots'])
    """
    _matches_data = []

    for match_id in _league_data_ext["Match ID"].unique():

        _matches_data.append(
            {
                **load_match_rosters(match_id, _league_folder, _year, _league_data_ext),
                "match_id": match_id,
                "shots": load_match_shots(
                    match_id, _league_folder, _year, _league_data_ext
                ),
            }
        )

    return _matches_data


def teams_data(
    _league_folder: str,
    _year: str,
) -> list:
    """Load the teams data

    Args:
        _league_folder (str): The league folder
        _year (str): The year (Season start year)

    Returns:
        list: A list of teams_data
    """
    return [
        team_data(team_name, _league_folder, _year)
        for team_name in os.listdir(os.path.join(_league_folder, str(_year), "teams"))
        if os.path.isdir(os.path.join(_league_folder, str(_year), "teams", team_name))
    ]


def load_data(data_folder="output", leagues=None, years=None):
    """Load the data from the output folder"""
    if leagues is None:
        leagues = os.listdir(data_folder)
    # remove .DS_Store
    leagues = [league for league in leagues if league != ".DS_Store"]
    data = {}

    for league in leagues:
        data[league] = {}

        league_folder = os.path.join(data_folder, league)
        if years is None:
            years = os.listdir(league_folder)
            # year should be in the format of YYYY, castable to int
            years = [year for year in years if year.isdigit()]
        
        print(f"Loading data for {league} from {years}")

        for year in years:
            print(f"Loading data for {league} from {year}")
            league_data = pd.read_csv(
                os.path.join(league_folder, str(year), f"{league}_{year}.csv")
            )
            league_data_ext = pd.read_csv(
                os.path.join(league_folder, str(year), f"{league}_{year}_extended.csv")
            )

            data[league][year] = {
                "data": league_data,
                "data_ext": league_data_ext,
                "matches": matches_data(league_folder, year, league_data_ext),
                "teams": teams_data(league_folder, year),
            }

    return data


__all__ = [
    "load_data",
    "load_match_rosters",
    "load_match_shots",
    "load_team_statistics",
    "load_player_id",
    "load_player_matches",
    "load_player_shots",
    "load_teams_groups_json",
    "player_data",
    "team_data",
    "matches_data",
    "teams_data",
]
