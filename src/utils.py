import requests
import bz2

import os
import subprocess

from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("OPENDOTA_API_KEY")
API_URL = "https://api.opendota.com/api/"


def get_public_matches(min_rank=80, game_mode=22, lobby_type=7):
    """
    Get public matches by min rank, game mode, and lobby type
    (by default, get all ranked all pick matches for Immortal players)

    Data format for single match:
    {
    "match_id": 8226246802,
    "match_seq_num": 6912775440,
    "radiant_win": true,
    "start_time": 1742744300,
    "duration": 1551,
    "lobby_type": 7,
    "game_mode": 22,
    "avg_rank_tier": 81,
    "num_rank_tier": 7,
    "cluster": 273,
    "radiant_team": [10, 97, 29, 86, 51],
    "dire_team": [112, 22, 13, 54, 129]
    }
    """
    request = API_URL + "publicMatches" + "?" + f"min_rank={min_rank}" + "&" + f"game_mode={game_mode}" + "&" + f"lobby_type={lobby_type}"
    response = requests.get(request)
    return response.json()


def get_matches_from_explorer(n=1, patch="7.37"):
    query = f"""
        SELECT 
        m.match_id,
        m.match_seq_num,
        m.replay_salt,
        mp.patch,
        m.game_mode,
        m.duration,
        m.start_time,
        m.cluster,
        m.radiant_win
        FROM matches m
        JOIN match_patch mp
            ON mp.match_id = m.match_id
            AND mp.patch = '{patch}'
        WHERE
        (m.game_mode = 1 OR m.game_mode = 2)
        ORDER BY m.start_time DESC
        LIMIT {n};
        """
    res = send_sql_query(query)
    if "rows" not in res:
        print("Error: ", res)
        return []
    return send_sql_query(query)["rows"]


def send_sql_query(query):
    """
    Send SQL query to OpenDota API
    """
    request = API_URL + "explorer" + "?" + f"sql={query}"
    response = requests.get(request)
    return response.json()


def get_match_data(id):
    """
    Get match data by match ID
    """
    request = API_URL + "matches/" + str(id)
    response = requests.get(request)
    return response.json()


def get_replay_url(cluster, match_id, replay_salt):
    """
    Get replay URL by match ID
    """
    replay_url = f"http://replay{cluster}.valve.net/570/{match_id}_{replay_salt}.dem.bz2"
    return replay_url


def download_replay(url, download_path="./replays"):
    """
    Download archived replay file from URL
    """
    replay_response = requests.get(url)
    filename = f"{download_path}/{url.split('/')[-1]}"
    if replay_response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(replay_response.content)
        print("Replay downloaded successfully!")
    else:
        print("Failed to download replay.")
    return filename


def decompress_replay(bz2_file, output_file):
    with bz2.BZ2File(bz2_file, 'rb') as fr, open(output_file, 'wb') as fw:
        fw.write(fr.read())
    os.remove(bz2_file)
    return


def parse_replay(replay_path, output_path):
    """
    Parse replay file using parser
    """
    command = ["java", "-jar", "./parser/replay-parser/target/replay-parser-1.0.0.jar", replay_path, output_path]
    try:
        subprocess.run(command, check=True)
        print(f"Replay parsed successfully! Output saved to {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while parsing replay: {e}")
        return
    return output_path


def send_odota_parse_request(replay_url):
    """
    Send a parse request to running local odota parser

    Same as running:
    curl localhost:5600/blob?replay_url={replay_url}
    """
    request = f"http://localhost:5600/blob?replay_url={replay_url}"
    response = requests.get(request)
    return response.json()
