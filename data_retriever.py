"""
A class to retrieve and download chess game archives for players from the Chess.com API. 
It manages the download of PGN files for specified players, 
validates the downloaded data, and handles errors.

Attributes:
-----------
source_config : dict
    Configuration dictionary containing player names and their corresponding output folders.

Methods:
--------
download_player_games(player: str, download_path: str):
    Downloads the game archives in PGN format for a specific player from Chess.com 
    and saves them to the specified path.

retrieve_data():
    Iterates through the list of players, checks if the output folder exists, 
    and triggers the download of player games.

validate_data():
    Checks if the downloaded files exist in the output folders for each player 
    and verifies successful data retrieval.

process_data():
    Orchestrates the entire data retrieval and validation process for all players.
"""
import urllib.request
import os

class DataRetriever:
    def __init__(self, source_config):
        self.output_folders = source_config['output_folders']
        self.players = source_config['players']
    
    def download_player_games(self, player, download_path):
        base_url = f"https://api.chess.com/pub/player/{player}/games/archives"
        
        # Step 1: read the archives and store these in a list
        try:
            with urllib.request.urlopen(base_url) as response:
                archives = response.read().decode("utf-8")
            archives_list = eval(archives)['archives']
        except Exception as e:
            print(f"\nError retrieving archives for {player}: {e}")
            return
        
        print(f"\nRetrieving pgn games for {player}")   
        # Step 2: download the games
        for archive_url in archives_list:
            url = f"{archive_url}/pgn"
            filename = "-".join(archive_url.split("/")[-2:])
            full_path = os.path.join(download_path, f"{filename}.pgn")
            try:
                urllib.request.urlretrieve(url, full_path)
                print(f"\n{filename}.pgn has been downloaded to {download_path}.")
            except Exception as e:
                print(f"\nError downloading {filename}.pgn for {player}: {e}")          
        print(f"\nAll files for {player} have been downloaded")     
    
    def retrieve_data(self):
        for player in self.players:
            if player in self.output_folders:
                download_path = self.output_folders[player]
                if not os.path.exists(download_path):
                    os.makedirs(download_path)
                self.download_player_games(player, download_path)
            else:
                print(f"No folder path defined for player: {player}")
    
    def validate_data(self):
        for player, folder in self.output_folders.items():
            if os.path.exists(folder) and os.listdir(folder):
                print(f"Data for {player} has been successfully downloaded and validated.")
            else:
                print(f"Data for {player} is missing or not downloaded correctly")
    
    def process_data(self):
        self.retrieve_data()
        self.validate_data()  
    
                  