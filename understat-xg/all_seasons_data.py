from datetime import datetime
import json
import re
import csv
import requests
import os


def get_json(url):
    """_summary_

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    response = requests.get(url, timeout=50)
    data = re.search("datesData\s+=\s+JSON.parse\('([^']+)", response.text)
    decoded_string = bytes(data.groups()[0], 'utf-8').decode('unicode_escape')
    season_json = json.loads(decoded_string)
    return season_json


def download_write_match_csv(match, league, year):

    # Under a /matches folder, download the table under f"https://understat.com/match/{match['match_id]}"
    # with the filename as the match_id-HomeTeam-AwayTeam.csv

    home_team = match['h']['title']
    away_team = match['a']['title']
    match_id = match['id']

    match_folder = os.path.join("output", league, str(year), "matches", str(match_id))
    os.makedirs(match_folder, exist_ok=True)

    match_filename = os.path.join(match_folder, f"{match_id}-{home_team}-{away_team}.csv")
    shots_filename = os.path.join(match_folder, f"{match_id}-{home_team}-{away_team}_shots.csv")
    rosters_filename = os.path.join(
        match_folder, f"{match_id}-{home_team}-{away_team}_rosters.csv"
    )

    if (
        os.path.exists(match_filename)
        and os.path.exists(shots_filename)
        and os.path.exists(rosters_filename)
    ):
        return

    match_url = f"https://understat.com/match/{match_id}"
    match_response = requests.get(match_url, timeout=50)

    match_data = re.search("match_info\s+=\s+JSON.parse\('([^']+)", match_response.text)
    decoded_match_string = bytes(match_data.groups()[0], 'utf-8').decode('unicode_escape')
    match_json = json.loads(decoded_match_string)

    extended_matches_data_filename = os.path.join(
        "output", league, str(year), f"{league}_{year}_extended.csv"
    )

    should_write_headers = not os.path.exists(extended_matches_data_filename)

    with open(
        extended_matches_data_filename, mode="a", encoding="utf-8"
    ) as f:

        """ 
        Object.keys(match_info):
            ['id', 'fid', 'h', 'a', 'date', 'league_id', 'season', 'h_goals', 'a_goals', 'team_h', 'team_a', 'h_xg', 'a_xg', 'h_w', 'h_d', 'h_l', 'league', 'h_shot', 'a_shot', 'h_shotOnTarget', 'a_shotOnTarget', 'h_deep', 'a_deep', 'a_ppda', 'h_ppda'] 
        """

        csv_writer = csv.writer(f)

        # Write headers

        if should_write_headers:
            csv_writer.writerow(
                [
                    'Match ID',
                    'Date',
                    'Home',
                    'Home Short',
                    'Away',
                    'Away Short',
                    'Home Goals',
                    'Away Goals',
                    'Home xG',
                    'Away xG',
                    'Home Shots',
                    'Away Shots',
                    'Home Shots on Target',
                    'Away Shots on Target',
                    'Home Deep',
                    'Away Deep',
                    'Away PPDA',
                    'Home PPDA'
                ]
            )

        csv_writer.writerow(
            [
                match_json['id'],
                match_json['date'],
                match_json['team_h'],
                match_json['h'],
                match_json['team_a'],
                match_json['a'],
                match_json['h_goals'],
                match_json['a_goals'],
                match_json['h_xg'],
                match_json['a_xg'],
                match_json['h_shot'],
                match_json['a_shot'],
                match_json['h_shotOnTarget'],
                match_json['a_shotOnTarget'],
                match_json['h_deep'],
                match_json['a_deep'],
                match_json['a_ppda'],
                match_json['h_ppda']
            ]
        )

    if not os.path.exists(shots_filename):
        shots_data = re.search("shotsData\s+=\s+JSON.parse\('([^']+)", match_response.text)
        decoded_shots_string = bytes(shots_data.groups()[0], "utf-8").decode(
            "unicode_escape"
        )
        shots_json = json.loads(decoded_shots_string)

        # Write shots data

        shots_filename = os.path.join(match_folder, f"{match_id}-{home_team}-{away_team}_shots.csv")

        # shorts is an object with a and h keys
        # each key is an array of objects with keys:
        """ 
        [
            "id",
            "minute",
            "result",
            "X",
            "Y",
            "xG",
            "player",
            "h_a",
            "player_id",
            "situation",
            "season",
            "shotType",
            "match_id",
            "h_team",
            "a_team",
            "h_goals",
            "a_goals",
            "date",
            "player_assisted",
            "lastAction",
        ] 
        """

        with open(shots_filename, mode="w", newline="", encoding="utf-8") as f:

            csv_writer = csv.writer(f)

            # Write headers
            csv_writer.writerow(
                [
                    'Team',
                    'Minute',
                    'Result',
                    'X',
                    'Y',
                    'xG',
                    'Player',
                    'Home/Away',
                    'Player ID',
                    'Situation',
                    'Season',
                    'Shot Type',
                    'Match ID',
                    'Home Team',
                    'Away Team',
                    'Home Goals',
                    'Away Goals',
                    'Date',
                    'Player Assisted',
                    'Last Action'
                ]
            )

            team_keys = ['a', 'h']

            for team_key in team_keys:
                for shot in shots_json[team_key]:
                    csv_writer.writerow(
                        [
                            team_key,
                            shot['minute'],
                            shot['result'],
                            shot['X'],
                            shot['Y'],
                            shot['xG'],
                            shot['player'],
                            shot['h_a'],
                            shot['player_id'],
                            shot['situation'],
                            shot['season'],
                            shot['shotType'],
                            shot['match_id'],
                            shot['h_team'],
                            shot['a_team'],
                            shot['h_goals'],
                            shot['a_goals'],
                            shot['date'],
                            shot['player_assisted'],
                            shot['lastAction']
                        ]
                    )

    if not os.path.exists(rosters_filename):
        rosters_data = re.search("rostersData\s+=\s+JSON.parse\('([^']+)", match_response.text)
        decoded_rosters_string = bytes(rosters_data.groups()[0], "utf-8").decode(
            "unicode_escape"
        )
        rosters_json = json.loads(decoded_rosters_string)

        # Write rosters data

        # rosters is an object with a and h keys
        # each key is an object with a bunch of keys
        # each key is an array of objects with keys:
        # ['id', 'goals', 'own_goals', 'shots', 'xG', 'time', 'player_id', 'team_id', 'position', 'player', 'h_a', 'yellow_card', 'red_card', 'roster_in', 'roster_out', 'key_passes', 'assists', 'xA', 'xGChain', 'xGBuildup', 'positionOrder']

        with open(rosters_filename, mode="w", newline="", encoding="utf-8") as f:

            csv_writer = csv.writer(f)

            # Write headers
            csv_writer.writerow(
                [
                    'Team',
                    'ID',
                    'Goals',
                    'Own Goals',
                    'Shots',
                    'xG',
                    'Time',
                    'Player ID',
                    'Team ID',
                    'Position',
                    'Player',
                    'Home/Away',
                    'Yellow Card',
                    'Red Card',
                    'Roster In',
                    'Roster Out',
                    'Key Passes',
                    'Assists',
                    'xA',
                    'xGChain',
                    'xGBuildup',
                    'Position Order',
                    'Match ID',
                    'Home Team',
                    'Away Team',
                    'Season'
                ]
            )

            team_keys = ['a', 'h']

            for team_key in team_keys:
                for roster in rosters_json[team_key].values():
                    csv_writer.writerow(
                        [
                            team_key,
                            roster['id'],
                            roster['goals'],
                            roster['own_goals'],
                            roster['shots'],
                            roster['xG'],
                            roster['time'],
                            roster['player_id'],
                            roster['team_id'],
                            roster['position'],
                            roster['player'],
                            roster['h_a'],
                            roster['yellow_card'],
                            roster['red_card'],
                            roster['roster_in'],
                            roster['roster_out'],
                            roster['key_passes'],
                            roster['assists'],
                            roster['xA'],
                            roster['xGChain'],
                            roster['xGBuildup'],
                            roster['positionOrder'],
                            match_id,
                            match_json['team_h'],
                            match_json['team_a'],
                            match_json['season']
                        ]
                    )

visited_players = {}

def write_season_csv(
    season_json,
    league,
    year,
    leagues,
    filename='xg.csv',
):
    """_summary_

    Args:
        season_json (_type_): _description_
        filename (str, optional): _description_. Defaults to 'xg.csv'.
    """
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)

        # Write headers
        csv_writer.writerow(
            [
                'Match ID',
                'Date',
                'Home',
                'Home Short',
                'Away',
                'Away Short',
                'Home Goals',
                'Away Goals',
                'Home xG',
                'Away xG'
            ]
        )

        teams = set()

        for match in season_json:
            csv_writer.writerow(
                [
                    match['id'],
                    match['datetime'],
                    match['h']['title'],
                    match['h']['short_title'],
                    match['a']['title'],
                    match['a']['short_title'],
                    match['goals']['h'],
                    match['goals']['a'],
                    match['xG']['h'],
                    match['xG']['a']
                ]
            )

            teams.add(match['h']['title'])

            try:
                download_write_match_csv(match, league, year)
            except Exception as e:
                print(f"Error downloading match {match['id']}, for League: {league}, Year: {year}: {e}")

        # for each team
        # make a folder under league/year/teams/{team}
        # write a csv file with the team's statistics

        # from https://understat.com/team/{team}/2023 (ex: https://understat.com/team/Manchester_City/2023)
        # get playersData and statisticsData
        # Save statisticsData json directly to a file

        for team_name in teams:

            try:
                parsed_team_name = team_name.replace(" ", "_")
                url = f"https://understat.com/team/{team_name}/{year}"

                team_folder = os.path.join("output", league, str(year), "teams", parsed_team_name)
                os.makedirs(team_folder, exist_ok=True)

                statistics_filename = os.path.join(team_folder, f"{parsed_team_name}_statistics.json")
                players_filename = os.path.join(team_folder, f"{parsed_team_name}_players.csv")

                if os.path.exists(statistics_filename) and os.path.exists(players_filename):
                    continue

                team_response = requests.get(url, timeout=50)

                # if request failed, skip
                if team_response.status_code != 200:
                    continue
                if "playersData" not in team_response.text or "statisticsData" not in team_response.text:
                    continue

                players_data = re.search("playersData\s+=\s+JSON.parse\('([^']+)", team_response.text)
                decoded_team_string = bytes(players_data.groups()[0], 'utf-8').decode('unicode_escape')
                players_data = json.loads(decoded_team_string)

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
                if not os.path.exists(players_filename):
                    with open(players_filename, mode="w", newline="", encoding="utf-8") as f:
                        csv_writer_i = csv.writer(f)

                        # Write headers
                        csv_writer_i.writerow(
                            [
                                "Player ID",
                                "Player Name",
                                "Games",
                                "Time",
                                "Goals",
                                "xG",
                                "Assists",
                                "xA",
                                "Shots",
                                "Key Passes",
                                "Yellow Cards",
                                "Red Cards",
                                "Position",
                                "Team",
                                "NPG",
                                "NPxG",
                                "xGChain",
                                "xGBuildup",
                            ]
                        )

                        for player in players_data:

                            player_id = player['id']
                            player_name = player['player_name'].replace(" ", "_")

                            # Player allready visited, or player folder allready exists somewhere in the other teams/leagues/prev_years folder, make alias

                            # if (player_id in visited_players):
                            #     # make alias of the player folder into this team folder
                            #     player_folder = visited_players[player_id]

                            #     if player_folder == os.path.join(
                            #         team_folder,
                            #         f"{player_id}-{player_name}",
                            #     ):
                            #         continue
                                
                            #     os.symlink(
                            #         player_folder,
                            #         os.path.join(
                            #             team_folder,
                            #             f"{player_id}-{player_name}",
                            #         ),
                            #         target_is_directory=True,
                            #     )
                            #     continue
                            # if len(
                            #     (subpaths := [    
                            #         subpath
                            #         for team in teams
                            #         for _league in leagues
                            #         for _year in range(2014, year)
                            #         if os.path.exists(subpath := os.path.join(
                            #             "output", _league, str(_year), "teams", team, player_name
                            #         ))
                            #     ])
                            # ) > 0:
                            #     # make alias of the player folder into this team folder
                            #     player_folder = subpaths[0]
                            #     os.symlink(
                            #         player_folder,
                            #         os.path.join(
                            #             team_folder,
                            #             player_name,
                            #         ),
                            #         target_is_directory=True,
                            #     )
                            #     continue
                            # else:
                            try:
                                csv_writer_i.writerow(
                                    [
                                        player["id"],
                                        player["player_name"],
                                        player["games"],
                                        player["time"],
                                        player["goals"],
                                        player["xG"],
                                        player["assists"],
                                        player["xA"],
                                        player["shots"],
                                        player["key_passes"],
                                        player["yellow_cards"],
                                        player["red_cards"],
                                        player["position"],
                                        player["team_title"],
                                        player["npg"],
                                        player["npxG"],
                                        player["xGChain"],
                                        player["xGBuildup"],
                                    ]
                                )

                                # save groupsData as a json file
                                # save shotsData as a csv file
                                # save matchesData as a csv file

                                player_folder = os.path.join(team_folder, f"{player_id}-{player_name}")
                                os.makedirs(player_folder, exist_ok=True)

                                visited_players[player_id] = player_folder

                                player_url = f"https://understat.com/player/{player_id}"
                                player_response = requests.get(player_url, timeout=50)
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
                                        csv_writer_i2 = csv.writer(f)

                                        # Write headers
                                        csv_writer_i2.writerow(
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
                                            csv_writer_i2.writerow(
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
                                        csv_writer_i3 = csv.writer(f)

                                        # Write headers
                                        csv_writer_i3.writerow(
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
                                            csv_writer_i3.writerow(
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
                                print(f"Error downloading player {player['player_name']} id: {player['id']}, for Team: {team_name}, League: {league}, Year: {year}: {e}")

            except Exception as e:
                print(f"Error downloading team {team_name}, for League: {league}, Year: {year}: {e}")

        # from https://understat.com/player/{player_id} (ex: https://understat.com/player/8260)
        # get groupsData, shotsData and matchesData

        # shotsData[0] example:
        """ 
        {
            "id": "354876",
            "minute": "58",
            "result": "Goal",
            "X": "0.8880000305175781",
            "Y": "0.6659999847412109",
            "xG": "0.07933320105075836",
            "player": "Erling Haaland",
            "h_a": "a",
            "player_id": "8260",
            "situation": "OpenPlay",
            "season": "2019",
            "shotType": "LeftFoot",
            "match_id": "12562",
            "h_team": "Augsburg",
            "a_team": "Borussia Dortmund",
            "h_goals": "3",
            "a_goals": "5",
            "date": "2020-01-18 14:30:00",
            "player_assisted": "Jadon Sancho",
            "lastAction": "Throughball"
            }
        """

        # matchesData[0] example:
        """ 
        {
            "goals": "0",
            "shots": "4",
            "xG": "0.9477797150611877",
            "time": "90",
            "position": "FW",
            "h_team": "Manchester City",
            "a_team": "West Ham",
            "h_goals": "3",
            "a_goals": "1",
            "date": "2024-05-19",
            "id": "22273",
            "season": "2023",
            "roster_id": "662999",
            "xA": "0",
            "assists": "0",
            "key_passes": "0",
            "npg": "0",
            "npxG": "0.9477797150611877",
            "xGChain": "0.5510027408599854",
            "xGBuildup": "0.26654958724975586"
            }
        """


def main():
    """
    """

    if os.path.exists(os.path.join("output")):
        # prompt user to continue
        if input("Output folder exists, continue? (y/n): ") != "y":
            return

    # leagues = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1", "RFPL"]
    leagues = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]
    _ = datetime
    # current_year = datetime.now().year
    # years = list(range(2014, current_year + 1))
    years = list(range(2014, 2024))
    skip_full_years = True

    for i, league in enumerate(leagues):
        if i < 2:
            continue
        for year in years:
            if i == 2 and year < 2016:
                continue

            # if output/{league}/{year+1}/ exists, skip
            if skip_full_years and os.path.exists(
                os.path.join("output", league, str(year + 1))
            ):
                continue

            try:
                url = f"https://understat.com/league/{league}/{year}"
                season_json = get_json(url)
                folder_path = os.path.join("output", league, str(year))
            except Exception as e:
                print(f"Error downloading {league} {year}: {e}")
            else:
                os.makedirs(folder_path, exist_ok=True)
                filename = os.path.join(folder_path, f"{league}_{year}.csv")
                write_season_csv(season_json, league, year, leagues, filename)


if __name__ == '__main__':
    main()
