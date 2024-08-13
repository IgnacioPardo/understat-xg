import csv
import json
import os
import re
import requests
import pandas


league = "La Liga".replace(" ", "_")
year = 2023

df = pandas.read_csv(f"output/{league}/{year}/{league}_{year}.csv")
teams = set(df["Home"].unique()) | set(df["Away"].unique())

for team_name in teams:
    try:
        parsed_team_name = team_name.replace(" ", "_")
        url = f"https://understat.com/team/{team_name}/{year}"

        team_folder = os.path.join("output", league, str(year), "teams", parsed_team_name)
        os.makedirs(team_folder, exist_ok=True)

        statistics_filename = os.path.join(team_folder, f"{parsed_team_name}_statistics.json")
        players_filename = os.path.join(team_folder, f"{parsed_team_name}_players.csv")

        team_response = requests.get(url, timeout=10)
        if team_response.status_code != 200:
            continue
        if "playersData" not in team_response.text or "statisticsData" not in team_response.text:
            continue

        players_data = re.search("playersData\s+=\s+JSON.parse\('([^']+)", team_response.text)
        decoded_team_string = bytes(players_data.groups()[0], 'utf-8').decode('unicode_escape')
        players_data = json.loads(decoded_team_string)
        print(players_data)

        statistics_data = re.search("statisticsData\s+=\s+JSON.parse\('([^']+)", team_response.text)
        decoded_statistics_string = bytes(statistics_data.groups()[0], 'utf-8').decode('unicode_escape')
        statistics_data = json.loads(decoded_statistics_string)
        # write statistics_data as a json file
        if not os.path.exists(statistics_filename):
            with open(statistics_filename, mode="w", encoding="utf-8") as f:
                json.dump(statistics_data, f, indent=4)

        # Players data is an array of objects

        # playersData[0] example:
        """
        {
            "id": "8260",
            "player_name": "Erling Haaland",
            "games": "31",
            "time": "2581",
            "goals": "27",
            "xG": "31.65399668365717",
            "assists": "5",
            "xA": "4.7517555598169565",
            "shots": "122",
            "key_passes": "29",
            "yellow_cards": "1",
            "red_cards": "0",
            "position": "F S",
            "team_title": "Manchester City",
            "npg": "20",
            "npxG": "25.564646281301975",
            "xGChain": "30.19725350290537",
            "xGBuildup": "3.128645434975624"
        }
        """
        with open(players_filename, mode="w", newline="", encoding="utf-8") as f:
            csv_writer = csv.writer(f)

            # Write headers
            csv_writer.writerow(
                [
                    'Player Name',
                    'Games',
                    'Time',
                    'Goals',
                    'xG',
                    'Assists',
                    'xA',
                    'Shots',
                    'Key Passes',
                    'Yellow Cards',
                    'Red Cards',
                    'Position',
                    'Team',
                    'NPG',
                    'NPxG',
                    'xGChain',
                    'xGBuildup'
                ]
            )

            for player in players_data:
                try:
                    print(player['player_name'], player["id"])
                    csv_writer.writerow(
                        [
                            player['player_name'],
                            player['games'],
                            player['time'],
                            player['goals'],
                            player['xG'],
                            player['assists'],
                            player['xA'],
                            player['shots'],
                            player['key_passes'],
                            player['yellow_cards'],
                            player['red_cards'],
                            player['position'],
                            player['team_title'],
                            player['npg'],
                            player['npxG'],
                            player['xGChain'],
                            player['xGBuildup']
                        ]
                    )

                    player_id = player['id']
                    player_name = player['player_name'].replace(" ", "_")

                    # save groupsData as a json file
                    # save shotsData as a csv file
                    # save matchesData as a csv file

                    player_folder = os.path.join(team_folder, player_name)
                    os.makedirs(player_folder, exist_ok=True)

                    player_url = f"https://understat.com/player/{player_id}"
                    player_response = requests.get(player_url, timeout=10)
                    if player_response.status_code != 200:
                        continue
                    if (
                        "groupsData" not in player_response.text
                        or "shotsData" not in player_response.text
                        or "matchesData" not in player_response.text
                    ):
                        continue

                    groups_data = re.search(
                        "groupsData\s+=\s+JSON.parse\('([^']+)", player_response.text
                    )
                    decoded_groups_string = bytes(groups_data.groups()[0], "utf-8").decode(
                        "unicode_escape"
                    )
                    groups_data = json.loads(decoded_groups_string)

                    groups_filename = os.path.join(
                        player_folder, f"{player_id}_{player_name}_groups.json"
                    )
                    if not os.path.exists(groups_filename):
                        with open(groups_filename, mode="w", encoding="utf-8") as f:
                            json.dump(groups_data, f, indent=4)

                    shots_data = re.search(
                        "shotsData\s+=\s+JSON.parse\('([^']+)", player_response.text
                    )
                    decoded_shots_string = bytes(shots_data.groups()[0], "utf-8").decode(
                        "unicode_escape"
                    )
                    shots_data = json.loads(decoded_shots_string)

                    shots_filename = os.path.join(
                        player_folder, f"{player_id}_{player_name}_shots.csv"
                    )
                    if not os.path.exists(shots_filename):
                        with open(shots_filename, mode="w", newline="", encoding="utf-8") as f:
                            csv_writer_i = csv.writer(f)

                            # Write headers
                            csv_writer_i.writerow(
                                [
                                    "Minute",
                                    "Result",
                                    "X",
                                    "Y",
                                    "xG",
                                    "Situation",
                                    "Season",
                                    "Shot Type",
                                    "Match ID",
                                    "Home Team",
                                    "Away Team",
                                    "Home Goals",
                                    "Away Goals",
                                    "Date",
                                    "Player Assisted",
                                    "Last Action",
                                ]
                            )

                            for shot in shots_data:
                                csv_writer_i.writerow(
                                    [
                                        shot["minute"],
                                        shot["result"],
                                        shot["X"],
                                        shot["Y"],
                                        shot["xG"],
                                        shot["situation"],
                                        shot["season"],
                                        shot["shotType"],
                                        shot["match_id"],
                                        shot["h_team"],
                                        shot["a_team"],
                                        shot["h_goals"],
                                        shot["a_goals"],
                                        shot["date"],
                                        shot["player_assisted"],
                                        shot["lastAction"],
                                    ]
                                )

                    matches_data = re.search(
                        "matchesData\s+=\s+JSON.parse\('([^']+)", player_response.text
                    )
                    decoded_matches_string = bytes(matches_data.groups()[0], "utf-8").decode(
                        "unicode_escape"
                    )
                    matches_data = json.loads(decoded_matches_string)

                    matches_filename = os.path.join(
                        player_folder, f"{player_id}_{player_name}_matches.csv"
                    )
                    if not os.path.exists(matches_filename):
                        with open(
                            matches_filename, mode="w", newline="", encoding="utf-8"
                        ) as f:
                            csv_writer_i2 = csv.writer(f)

                            # Write headers
                            csv_writer_i2.writerow(
                                [
                                    "Goals",
                                    "Shots",
                                    "xG",
                                    "Time",
                                    "Position",
                                    "Home Team",
                                    "Away Team",
                                    "Home Goals",
                                    "Away Goals",
                                    "Date",
                                    "ID",
                                    "Season",
                                    "Roster ID",
                                    "xA",
                                    "Assists",
                                    "Key Passes",
                                    "NPG",
                                    "NPxG",
                                    "xGChain",
                                    "xGBuildup",
                                ]
                            )

                            for match in matches_data:
                                csv_writer_i2.writerow(
                                    [
                                        match["goals"],
                                        match["shots"],
                                        match["xG"],
                                        match["time"],
                                        match["position"],
                                        match["h_team"],
                                        match["a_team"],
                                        match["h_goals"],
                                        match["a_goals"],
                                        match["date"],
                                        match["id"],
                                        match["season"],
                                        match["roster_id"],
                                        match["xA"],
                                        match["assists"],
                                        match["key_passes"],
                                        match["npg"],
                                        match["npxG"],
                                        match["xGChain"],
                                        match["xGBuildup"],
                                    ]
                                )
                except Exception as e:
                    print(e)
    except Exception as e:
        print(e)
