from typing import Dict, List, Tuple
import pandas as pd
from create_data import load_data
import numpy as np


# Dataset for every player, match and team in the EPL 2023 season
# Initialize the data structure


# Get all matches from the dataframe
def matches_data(Leage_Data):
    swap_counts = 0
    precision_errors = 0
    errors = []

    matches_data = {}

    for year in Leage_Data.keys():
        matches_df = Leage_Data[year]["data_ext"]
        # Iterate through each match in the dataframe
        for _, match in matches_df.iterrows():
            match_id = match["Match ID"]

            # Get team names
            home_team_name = match["Home"].replace(" ", "_")
            away_team_name = match["Away"].replace(" ", "_")

            # Get team IDs from the dataframe
            home_team_id = match["Home Short"]  # Assuming "Home Short" is used as the team ID
            away_team_id = match["Away Short"]  # Assuming "Away Short" is used as the team ID

            # Initialize the match entry in the dictionary

            # match (extended_data) keys
            # Index(['Match ID', 'Date', 'Home', 'Home Short', 'Away', 'Away Short',
            #   'Home Goals', 'Away Goals', 'Home xG', 'Away xG', 'Home Shots',
            #   'Away Shots', 'Home Shots on Target', 'Away Shots on Target',
            #   'Home Deep', 'Away Deep', 'Away PPDA', 'Home PPDA'],
            #  dtype='object')

            # roster summary keys
            #  Index(['Home Goals', 'Home Own Goals', 'Home Shots', 'Home xG', 'Home Time',
            #   'Home Yellow Card', 'Home Red Card', 'Home Key Passes', 'Home Assists',
            #   'Home xA', 'Home xGChain', 'Home xGBuildup', 'Away Goals',
            #   'Away Own Goals', 'Away Shots', 'Away xG', 'Away Time',
            #   'Away Yellow Card', 'Away Red Card', 'Away Key Passes', 'Away Assists',
            #   'Away xA', 'Away xGChain', 'Away xGBuildup', 'Season', 'Match ID',
            #   'League'],
            #  dtype='object')

            matches_data[match_id] = {
                "extended_data": match,
                "date": match["Date"],
                "home_team": {
                    "team_name": home_team_name,
                    "team_id": home_team_id,
                    "players": {},
                },
                "away_team": {
                    "team_name": away_team_name,
                    "team_id": away_team_id,
                    "players": {},
                },
                "roster": {},
                "roster_summary": {},
                "shots": {},
                "data_errors": 0,
                "full_data" : None
            }

            # Find the corresponding match in Leage_Data["2023"]["matches"]
            match_detail = next(
                (m for m in Leage_Data["2023"]["matches"] if m["match_id"] == match_id), None
            )

            home_team = next(
                (
                    team
                    for team in Leage_Data["2023"]["teams"]
                    if team["team_name"] == home_team_name
                ),
                None,
            )

            away_team = next(
                (
                    team
                    for team in Leage_Data["2023"]["teams"]
                    if team["team_name"] == away_team_name
                ),
                None,
            )

            if not home_team or not away_team:
                # print(f"Team {home_team_name} or {away_team_name} not found")
                continue

            if match_detail:
                # Add roster summary and shots to the match data
                matches_data[match_id]["roster_summary"] = match_detail.get(
                    "rosters_summary", {}
                )
                matches_data[match_id]["shots"] = match_detail.get("shots", {})
                matches_data[match_id]["roster"] = match_detail.get("rosters", {})

                # Process home team players
                # print(match_detail["rosters"].keys())
                # print(match_detail["rosters"]["Home/Away"].value_counts())
                # break

                home_team_players = match_detail["rosters"][
                    match_detail["rosters"]["Home/Away"] == "h"
                ]

                for _, player_in_roster in home_team_players.iterrows():
                    player_id = player_in_roster["Player ID"]

                    # print(home_team["players"][0].keys())
                    # print([player["player_id"] for player in home_team["players"]])
                    # print(player_id)
                    player_obj = next(
                        (
                            player
                            for player in home_team["players"]
                            if player["player_id"] == str(player_id)
                        ),
                        None,
                    )

                    if not player_obj:
                        # print(f"Player {player_id} not found in team {home_team_name}")
                        continue
                    # print(player_obj.keys())
                    # [season for season in Haaland["groups"]["season"] if season["season"] == "2023"][0]
                    player_season = next(
                        (
                            season
                            for season in player_obj["groups"]["season"]
                            if season["season"] == "2023"
                        ),
                        None,
                    )

                    matches_data[match_id]["home_team"]["players"][player_id] = {
                        "player_name": player_in_roster["Player"],
                        "player_roster": player_in_roster,
                        "player_season": player_season,
                    }

                # Process away team players
                away_team_players = match_detail["rosters"][
                    match_detail["rosters"]["Home/Away"] == "a"
                ]

                for _, player_in_roster in away_team_players.iterrows():
                    player_id = player_in_roster["Player ID"]

                    # print([player["player_id"] for player in away_team["players"]])
                    # print(player_id)

                    player_obj = next(
                        (
                            player
                            for player in away_team["players"]
                            if player["player_id"] == str(player_id)
                        ),
                        None,
                    )

                    if not player_obj:
                        # print(f"Player {player_id} not found in team {away_team_name}")
                        continue

                    player_season = next(
                        (
                            season
                            for season in player_obj["groups"]["season"]
                            if season["season"] == "2023"
                        ),
                        None,
                    )

                    matches_data[match_id]["away_team"]["players"][player_id] = {
                        "player_name": player_in_roster["Player"],
                        "player_roster": player_in_roster,
                        "player_season": player_season,
                    }

                # Check if the cols that are in the match ext data and in the roster summary have the same values
                for col in match.keys():
                    if col in matches_data[match_id]["roster_summary"].keys():
                        if match[col] != matches_data[match_id]["roster_summary"][col][0]:
                            if col.startswith("Home") or col.startswith("Away"):
                                # Invertir los valores
                                col_suffix = col.split(" ", 1)[1]
                                if col.startswith("Home"):
                                    col_alt = f"Away {col_suffix}"
                                else:
                                    col_alt = f"Home {col_suffix}"

                                error_type = 0

                                def is_numeric(value):
                                    try:
                                        float(value)
                                        return True
                                    except ValueError:
                                        return False

                                if is_numeric(match[col]) and is_numeric(
                                    matches_data[match_id]["roster_summary"][col_alt][0]
                                ):
                                    match_value = float(match[col])
                                    roster_value = float(
                                        matches_data[match_id]["roster_summary"][col_alt][0]
                                    )
                                else:
                                    match_value = match[col]
                                    roster_value = matches_data[match_id]["roster_summary"][
                                        col_alt
                                    ][0]

                                if is_numeric(match[col_alt]) and is_numeric(
                                    matches_data[match_id]["roster_summary"][col][0]
                                ):
                                    match_value_alt = float(match[col_alt])
                                    roster_value_alt = float(
                                        matches_data[match_id]["roster_summary"][col][0]
                                    )
                                else:
                                    match_value_alt = match[col_alt]
                                    roster_value_alt = matches_data[match_id]["roster_summary"][
                                        col
                                    ][0]

                                decimal_places = 1
                                if match_value != roster_value:
                                    if round(match_value, decimal_places) == round(
                                        roster_value, decimal_places
                                    ):
                                        precision_errors += 1
                                        # Keep the value with more precision
                                        if len(str(match_value).split(".")[1]) > len(
                                            str(roster_value).split(".")[1]
                                        ):
                                            roster_value = match_value
                                        else:
                                            match_value = roster_value
                                    elif round(match_value, decimal_places) == round(
                                        roster_value_alt, decimal_places
                                    ):
                                        swap_counts += 1
                                        # Keep the value with more precision
                                        if len(str(match_value).split(".")[1]) > len(
                                            str(roster_value_alt).split(".")[1]
                                        ):
                                            roster_value_alt = match_value
                                        else:
                                            match_value = roster_value_alt
                                    else:
                                        print(
                                            f"{match_value} - {roster_value} | {match_value_alt} - {roster_value_alt}"
                                        )
                                        matches_data[match_id]["data_errors"] += 1
                                        # in case of error
                                        errors.append(1)

                                if match_value_alt != roster_value_alt:
                                    if round(match_value_alt, decimal_places) == round(
                                        roster_value_alt, decimal_places
                                    ):
                                        precision_errors += 1
                                        # Keep the value with more precision
                                        if len(str(match_value_alt).split(".")[1]) > len(
                                            str(roster_value_alt).split(".")[1]
                                        ):
                                            roster_value_alt = match_value_alt
                                        else:
                                            match_value_alt = roster_value_alt
                                    elif round(match_value_alt, decimal_places) == round(
                                        roster_value, decimal_places
                                    ):
                                        swap_counts += 1
                                        # Keep the value with more precision
                                        if len(str(match_value_alt).split(".")[1]) > len(
                                            str(roster_value).split(".")[1]
                                        ):
                                            roster_value = match_value_alt
                                        else:
                                            match_value_alt = roster_value
                                    else:
                                        print(
                                            f"{match_value_alt} - {roster_value_alt} | {match_value} - {roster_value}"
                                        )
                                        matches_data[match_id]["data_errors"] += 1
                                        errors.append(2)
                            else:

                                errors.append(3)

                # intersect the match ext data with the match roster summary
                matches_data[match_id]["full_data"] = pd.DataFrame(
                    {
                        **match,
                        **matches_data[match_id]["roster_summary"],
                    },
                    index=[0],
                )

    return matches_data

# print(f"Precision errors: {precision_errors}")
# print(f"Errors: {len(errors)} {pd.Series(errors).value_counts().to_dict()}")
# print(f"Swaps: {swap_counts}")

# Predictive Model

# The Model Takes the following inputs (arrays - full_data (match ^ roster_summary)):

# 1. The last 5 matches of the home team
# 2. The last 5 matches of the away team


# Other options for later:
# 3. The last 5 matches of the home team at home
# 4. The last 5 matches of the away team away
# 5. The last 5 matches between the two teams

# The Model Outputs the following (softmax) probabilities:
# 1. The probability of the home team winning
# 2. The probability of a draw
# 3. The probability of the away team winning

# The Model is trained on the 2021 season data
# The Model is tested on the 2022 season data

def create_dataset(matches_data_full: Dict[str, Dict]) -> Tuple[np.ndarray, np.ndarray]:
    """

    Args:
        matches_data_full (Dict[str, pd.DataFrame]):
            key: match_id
            value:
            {
                "extended_data": match,
                "full_data": pd.DataFrame,
                "date": match["Date"],
                "home_team": {
                    "team_name": home_team_name,
                    "team_id": home_team_id,
                    "players": {},
                },
                "away_team": {
                    "team_name": away_team_name,
                    "team_id": away_team_id,
                    "players": {},
                },
                "roster": {},
                "roster_summary": {},
                "shots": {},
                "data_errors": 0,
            }

    Returns:
        np.ndarray: _description_
    """
    X = []
    y = []

    for match in matches_data_full.values():
        print(match.keys())
        match_data = match["full_data"]

        # home_team = match_data["Home"]
        # home_team_id = match["home_team"]["team_id"]
        # away_team = match_data["Away"]
        # away_team_id = match["away_team"]["team_id"]

        home_goals = match_data["Home Goals"]
        away_goals = match_data["Away Goals"]

        # Get the previous matches of the home team
        prev_ms = lambda m_data, team: pd.concat(
            [
                p_match["full_data"]
                for p_match in matches_data_full.values()
                if (
                    (
                        (p_match["full_data"]["Home"] == m_data["full_data"][team])
                        or (p_match["full_data"]["Away"] == m_data["full_data"][team])
                    )
                    and (p_match["date"] < m_data["date"])
                )
            ]
        ).sort_values(by="Date", ascending=False)

        prev_home_ms: pd.DataFrame = prev_ms(match, "Home")
        prev_away_ms: pd.DataFrame = prev_ms(match, "Away")

        # Get the last 5 matches
        prev_home_n_ms = prev_home_ms.head(5)
        prev_away_n_ms = prev_away_ms.head(5)

        # Get the last 5 matches between the two teams
        get_last_between = lambda m_data: pd.concat(
            [
                p_match["full_data"]
                for p_match in matches_data_full.values()
                if (
                    (
                        (
                            (
                                p_match["full_data"]["Home"]
                                == m_data["full_data"]["Home"]
                            )
                            and (
                                p_match["full_data"]["Away"]
                                == m_data["full_data"]["Away"]
                            )
                        )
                        or (
                            (
                                p_match["full_data"]["Home"]
                                == m_data["full_data"]["Away"]
                            )
                            and (
                                p_match["full_data"]["Away"]
                                == m_data["full_data"]["Home"]
                            )
                        )
                    )
                    and (p_match["date"] < m_data["date"])
                )
            ]
        ).sort_values(by="Date", ascending=False)
   
        prev_home_vs_away_n_ms = get_last_between(match).head(5)

        # Create the input data

        Xi = pd.concat([prev_home_n_ms, prev_away_n_ms, prev_home_vs_away_n_ms]).values

        # Create the output data
        yi = np.ndarray([
            int(home_goals > away_goals),
            int(home_goals == away_goals),
            int(home_goals < away_goals),
        ])

        X.append(Xi)
        y.append(yi)

    return np.array(X), np.array(y)


if __name__ == "__main__":

    # Get the data
    data = load_data("output", ["EPL"])
    EPL_Data = data["EPL"]

    X, y = create_dataset(matches_data(EPL_Data))

    print(X.shape, y.shape)
