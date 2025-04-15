import json
import logging
import concurrent.futures
from tqdm import tqdm

from utils import *


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def sample_matches(n):
    sql_match_data = get_matches_from_explorer(n=n, patch="7.37")
    with open('./data/match_data.json', 'w') as f:
        json.dump(sql_match_data, f)


def process_match(match):
    if match['duration'] < 1200 or match['duration'] > 3600:
        return None

    match_id = match["match_id"]
    cluster = match["cluster"]
    replay_salt = match["replay_salt"]
    replay_url = get_replay_url(cluster, match_id, replay_salt)
    try:
        logging.info(f"Processing match ID: {match_id}")
        logging.info(f"Downloading replay - URL: {replay_url} ...")
        replay_file = download_replay(replay_url, download_path="./data/replays")
        
        decompressed_file = replay_file[:-4]
        logging.info(f"Decompressing replay - File: {replay_file} ...")
        decompress_replay(replay_file, decompressed_file)
        logging.info(f"Replay decompressed - File: {decompressed_file}")

        parsed_path = parse_replay(decompressed_file, output_path=f"./data/parsed_replays/{match_id}.json")
        os.remove(decompressed_file)

        if not parsed_path:
            return match_id
    except Exception as e:
        logging.info(f"Error processing match {match_id}: {e}")
        return match_id
    return None


def parse_replays_parallel():
    with open('./data/match_data.json', 'r') as f:
        match_data = json.load(f)
        logging.info(f"Sampled number of replays in data: {len(match_data)}")
    
    max_workers = 6
    failed_matches_local = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(process_match, match_data), total=len(match_data)))
    
    for result in results:
        if result is not None:
            failed_matches_local.append(result)
    
    logging.info("Done!")
    logging.info(f"Failed to parse {len(failed_matches_local)} matches.")
    with open('./data/failed_matches.json', 'w') as f:
        json.dump(failed_matches_local, f)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = TqdmLoggingHandler()
    formatter = logging.Formatter('[%(asctime)s] %(threadName)s: %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    NUM_MATCHES = 12000
    
    failed_matches = []
    os.makedirs('./data/replays', exist_ok=True)
    os.makedirs('./data/parsed_replays', exist_ok=True)

    logging.info("Sampling matches from SQL database...")
    sample_matches(NUM_MATCHES)
    logging.info("Done sampling matches!")

    parse_replays_parallel()
