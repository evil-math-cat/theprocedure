"""
The DataManipulator class is designed to process chess PGN (Portable Game Notation) files 
for specific players, generate relevant data, and filter that data based on game types 
(e.g., Blitz). 
It also calculates and saves winning streaks, game details, and other relevant statistics.

Main Functionality:
    1. Convert PGN files into pandas DataFrames.
    2. Filter games by time control (Blitz).
    3. Calculate winning streaks and provide details for each streak.
    4. Save the processed data to CSV files.

Attributes:
    input_pgn_files (dict): A dictionary mapping player keys to their respective input PGN file paths.
    output_dataframe_files (dict): A dictionary mapping player keys to the output CSV file paths for the DataFrame.
    output_dataframe_files_logs (dict): A dictionary mapping player keys to log file paths for skipped games.
    dataframe_files_filtered_by_blitz (dict): A dictionary mapping player keys to CSV files that contain only Blitz games.
    processed_streaks_unordered (dict): A dictionary mapping player keys to CSV files that contain calculated streaks in unordered format.
    processed_streaks_ordered (dict): A dictionary mapping player keys to CSV files that contain ordered streaks.
    processed_details (dict): A dictionary mapping player keys to CSV files that store the details of each streak.
    winner_names (dict): A dictionary mapping player keys to the appropriate winner's name, as it appears in the PGN files.

Methods:
    get_player_key(player_names):
        Returns the key corresponding to the player name by matching it with known aliases.
    
    is_valid_elo(elo_str):
        Checks if the given ELO string is a valid number.
    
    match_player_name(header_name, names):
        Matches the player's name from PGN headers to known aliases.
    
    create_player_dataframe(pgn_file, player_key, log_file):
        Reads a PGN file, processes the games, and generates a DataFrame.
    
    convert_pgn_files_to_dataframes():
        Converts PGN files for all players to CSV DataFrames.
    
    filter_by_blitz():
        Filters the DataFrames for games with Blitz time control and saves them to new files.
    
    calculate_streaks_and_details(df, player_name):
        Calculates the winning streaks and details for a specific player based on the provided DataFrame.
    
    process_files():
        Processes the filtered DataFrames and generates winning streaks and their details for each player.
    
    process_dataframes():
        Runs the entire processing pipeline, including conversion, filtering, and streak calculation.
    
Exceptions:
    ValueError: Raised when a player's name cannot be found in the player_names dictionary.
"""

import pandas as pd
import chess.pgn
import re
import os
import numpy as np

class DataManipulator:
    def __init__(self, input_pgn_files, 
                 output_dataframe_files, 
                 output_dataframe_files_logs, 
                 dataframe_files_filtered_by_blitz, 
                 processed_streaks_unordered, 
                 processed_streaks_ordered, 
                 processed_details, winner_names):
        self.input_pgn_files = input_pgn_files
        self.output_dataframe_files = output_dataframe_files
        self.output_dataframe_files_logs = output_dataframe_files_logs
        self.dataframe_files_filtered_by_blitz = dataframe_files_filtered_by_blitz
        self.processed_streaks_unordered = processed_streaks_unordered
        self.processed_streaks_ordered = processed_streaks_ordered
        self.processed_details = processed_details
        self.winner_names = winner_names
        self.dataframes = {}
        
        self.player_names = {
            2: ["Magnus Carlsen", "Carlsen", "Carlsen Magnus (NOR)", "Carlsen, Magnus", "Carlsen,M", 
                "Carlsen, M.", "Magnus", "Magnus C", "Magnus C.", "C., Magnus", "MagnusCarlsen"],
            1: ["Hikaru Nakamura", "Nakamura, Hi", "Nakamura, Hikaru", 
                "Hikaru", "Nakamura"],
            0: ["Fabiano Caruana", "Caruana, Fabiano", "Caruana, F.", "Fabiano C.",
                "Caruana Fabiano (ITA)", "Caruana","FabianoCaruana"]
        }
        
    def get_player_key(self, player_names):
        """Return the key corresponding to the player name."""
        for key, names in self.player_names.items():
            if any(name in player_names for name in names):
                return key
        raise ValueError(f"Player {player_names} not found in player_names.")


    def is_valid_elo(self, elo_str):
        """Check if the given ELO string is a valid number."""
        return elo_str.isdigit()
    
    def match_player_name(self, header_name, names):
        for name in names:
            name_parts = re.split(r'[,\s]+', name.lower())
            header_parts = re.split(r'[,\s]+', header_name.lower())
            if all(part in header_parts for part in name_parts):
                return True
        return False

    def create_player_dataframe(self, pgn_file, player_key, log_file):
        games_data = []
        not_found_ids = []
        skipped_games = []

        with open(pgn_file, 'r') as f:
            game_count = 0
            while True:
                game = chess.pgn.read_game(f)
                if game is None:
                    break
                
                game_count += 1
                headers = game.headers
                
                print(f"\nProcessing game {game_count}:")
                print(f"White player: {headers.get('White')}")
                print(f"Black player: {headers.get('Black')}")

                # Check for missing ELO
                white_elo = headers.get("WhiteElo", "")
                black_elo = headers.get("BlackElo", "")
                if not self.is_valid_elo(white_elo) or not self.is_valid_elo(black_elo):
                    print(f"Invalid ELO in game {headers.get('ID')}. Skipping...")
                    skipped_games.append((headers.get('ID'), "Invalid ELO"))
                    continue

                # Check for missing Time_Control
                if "Time_Control" not in headers:
                    print(f"Missing TimeControl in game {headers.get('ID')}. Skipping...")
                    skipped_games.append((headers.get('ID'), "Missing TimeControl"))
                    continue
                
                player_color = None
                if self.match_player_name(headers.get("White", ""), self.player_names[player_key]):
                    player_color = "White"
                elif self.match_player_name(headers.get("Black", ""), self.player_names[player_key]):
                    player_color = "Black"
                
                if player_color is None:
                    print(f"Player not found in game {headers.get('ID')}. Skipping...")
                    print(f"Searched for these names: {self.player_names[player_key]}")
                    not_found_ids.append(headers.get('ID'))
                    continue

                print(f"Found player as {player_color}")

                game_id_str = headers.get("ID")
                if game_id_str is not None and game_id_str.isdigit():
                    game_id = int(game_id_str)
                else:
                    game_id = None
                    print(f"Invalid or missing ID for game {game_count}. Skipping...")
                    continue

                outcome = headers.get("Result")
                if outcome == "1-0":
                    winner = headers.get("White")
                elif outcome == "0-1":
                    winner = headers.get("Black")
                elif outcome == "1/2-1/2":
                    winner = "Draw"
                else:
                    winner = None

                # Convert ELO ratings to integers
                player_elo = int(white_elo) if player_color == "White" else int(black_elo)
                opponent_elo = int(black_elo) if player_color == "White" else int(white_elo)
                
                # Calculate ELO difference
                elo_difference = player_elo - opponent_elo
              
                game_data = {
                    'ID': headers.get("ID"),
                    'Player_Identification': self.player_names[player_key][0],
                    'Outcome': headers.get("Result"),
                    'Time_Control': headers.get("Time_Control"),
                    'Game_Setting': headers.get("Event"),
                    'White_Player': headers.get("White"),
                    'Black_Player': headers.get("Black"),
                    'Player_ELO': player_elo,                
                    'Opponent_ELO': opponent_elo,
                    'Number_Of_Moves': len(list(game.mainline_moves())),
                    'Date': headers.get("Date"),
                    'Winner': winner,
                    'ELO Difference In Terms of Player': elo_difference
                }
                
                games_data.append(game_data)
        
        print(f"\nTotal games processed: {game_count}")
        print(f"Games included in dataset: {len(games_data)}")
        print(f"Games where player wasn't found: {len(not_found_ids)}")
        print(f"Games skipped due to missing data: {len(skipped_games)}")
        
        with open(log_file, 'w') as log:
            log.write(f"Total games processed: {game_count}\n")
            log.write("Game IDs where player wasn't found:\n")
            for game_id in not_found_ids:
                log.write(f"{game_id}\n")
            log.write(f"Games included in dataset: {len(games_data)}\n")
            log.write("Games skipped due to missing data:\n")
            for game_id, reason in skipped_games:
                log.write(f"{game_id}: {reason}\n")
        
        return pd.DataFrame(games_data)

    def convert_pgn_files_to_dataframes(self):
        for player, pgn_file in self.input_pgn_files.items():
            self.player_key = self.get_player_key(player)
            
            # Get the corresponding output paths for the CSV and log files
            csv_file = self.output_dataframe_files[player]
            log_file = self.output_dataframe_files_logs[player]
            
            # Ensure the directories exist
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            # Create the DataFrame and save it
            df = self.create_player_dataframe(pgn_file, self.player_key, log_file)
            df.to_csv(csv_file, index=False)
            self.dataframes[player] = df
            print(f"\nConverted PGN for {player} to CSV: {csv_file}")
            print(f"Log file saved for {player} to: {log_file}")

    def filter_by_blitz(self):
        for player, input_path in self.output_dataframe_files.items():
            # Load the DataFrame from the CSV file
            df = pd.read_csv(input_path)
            
            # Filter the DataFrame to include only 'blitz' games
            filtered_df = df[df['Time_Control'] == 'blitz']
            
            # Ensure the directory for the output file exists
            output_path = self.dataframe_files_filtered_by_blitz[player]
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save the filtered DataFrame to the specified path
            filtered_df.to_csv(output_path, index=False)
            
            print(f"Filtered 'blitz' games for {player} and saved to: {output_path}")

    def calculate_streaks_and_details(self, df, player_name):
    # These variables are used to track the current streak of wins, 
    # details of each streak, and the last processed game ID.
        streaks = []
        details = []
        current_streak = 0
        streak_start_id = None
        streak_details = []
        last_id = None
        pending_draw = 0
        
    # we iterate over each row of the dataframe
    # df.itterrows() is a method from pandas library to iterate over dataframe rows as
    # (index, series) pairs. 
    # idx is for the position, row represents a single dataframe row
        for idx, row in df.iterrows():
            current_id = row['ID']
            winner = row['Winner']
            
            # Check if the current ID is in sequence or if it's the start of a new sequence
            is_sequential = (last_id is None) or (current_id == last_id + 1)
            
            if winner == player_name:
                if streak_start_id is None or not is_sequential:
                    # Start of a new streak
                    if current_streak > 1:
                        streaks.append(current_streak) # Adds the current streak count to the streaks list.
                        details.extend(streak_details) # Adds the details of the current streak to the details list.
                    # Initialize a new streak
                    current_streak = 1
                    streak_start_id = current_id
                    streak_details = [{
                        'ID': current_id,
                        'Opponent_ELO': row['Opponent_ELO'],
                        'ELO_Difference': row['ELO Difference In Terms of Player']
                    }]
                    pending_draw = 0
                else:
                    # Continuation of the current streak
                    current_streak += 1 + pending_draw
                    streak_details.extend([{
                        'ID': id,
                        # For each ID in the range, it retrieves the corresponding opponent's ELO from the DataFrame df.
                        'Opponent_ELO': df.loc[df['ID'] == id, 'Opponent_ELO'].values[0],
                        # and the elo difference for the given ID
                        'ELO_Difference': df.loc[df['ID'] == id, 'ELO Difference In Terms of Player'].values[0]
                       # This range covers all the IDs that fall between the last ID of the previous streak and the current ID.
                    } for id in range(last_id + 1, current_id + 1)]) # type: ignore
                    pending_draw = 0
            elif winner == 'Draw' and is_sequential:
            # Handle a draw in the middle of a streak
                if streak_start_id is not None:
                    pending_draw = 0.5
                    streak_details.append({
                        'ID': current_id,
                        'Opponent_ELO': row['Opponent_ELO'],
                        'ELO_Difference': row['ELO Difference In Terms of Player']
                    })
                else:
                    # Draw without a preceding win doesn't start a streak
                    pending_draw = 0
            else:
                # End of streak (loss or non-sequential ID)
                if current_streak > 1:
                    streaks.append(current_streak)
                    details.extend(streak_details)
                current_streak = 0
                streak_start_id = None
                streak_details = []
                pending_draw = 0
            
            last_id = current_id
        
        # Handle the last streak if it exists
        if current_streak > 1:
            streaks.append(current_streak)
            details.extend(streak_details)
        
        return streaks, details

    def create_combined_frequencies_dataframe(self, name, input_file):
        """
        Process a CSV file to calculate frequencies of streak values and save the results.
        
        Args:
        name (str): The name to use for the output file.
        input_file (str): The path to the input CSV file.
        """
        # Load the dataset
        data = pd.read_csv(input_file)  # Assuming the files are in CSV format

        # Determine the maximum value in the 'Streak' column
        max_value = data['Streak'].max()

        # Generate Xi values starting from 2 up to the maximum value with a step of 0.5
        xi_values = np.arange(2, max_value + 0.5, 0.5).tolist()

        # Initialize an empty list to store frequency counts
        frequency_counts = []

        # Calculate frequencies for each Xi value
        for xi in xi_values:
            count = np.sum(data['Streak'] == xi)
            frequency_counts.append(count)

        # Create a new DataFrame for Xi and Fi
        result_df = pd.DataFrame({
            'Xi': xi_values,
            'Fi': frequency_counts
        })

        # Define the output file path
        output_file = f'C:/Users/diogo/Desktop/ML/1-Tools/0-Python scripts/chess_project2/data/3_visualization/{name}_frequencies.csv'

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save the new DataFrame to a CSV file
        result_df.to_csv(output_file, index=False)
        print(f'\nCSV file {output_file} created successfully.')

    def process_files(self):
        for player_key, input_file in self.dataframe_files_filtered_by_blitz.items():
            df = pd.read_csv(input_file)
            df = df.sort_values(by='ID').reset_index(drop=True)
            
            # Use the correct name for the 'Winner' column
            player_name_in_winner_column = self.winner_names[player_key]
            
            streaks, details = self.calculate_streaks_and_details(df, player_name_in_winner_column)
            
            #unordered versions
            streaks_df = pd.DataFrame({'Streak': streaks})
            streaks_df.to_csv(self.processed_streaks_unordered[player_key], index=False)
        
            details_df = pd.DataFrame(details)
            details_df.to_csv(self.processed_details[player_key], index=False)

            #ordered versions of streak to use in excel
            # Sort the DataFrame by the 'Streak' column in ascending order
            streaks_df_sorted_min_max = streaks_df.sort_values(by='Streak', ascending=True)
            # Save the sorted DataFrame to a CSV file
            streaks_df_sorted_min_max.to_csv(self.processed_streaks_ordered[player_key], index=False)
            
            # Create combined frequencies DataFrame
            self.create_combined_frequencies_dataframe(player_key, self.processed_streaks_ordered[player_key])
            
            print(f"Unordered Streaks saved to {self.processed_streaks_unordered[player_key]}")
            print(f"Ordered Streaks saved to {self.processed_streaks_ordered[player_key]}")
            print(f"Details saved to {self.processed_details[player_key]}")
            print(f"Combined player frequencies saved to {self.processed_details[player_key]}")


    def process_dataframes(self):   
        # Create the necessary DataFrames
        self.convert_pgn_files_to_dataframes()
        
        # Filter DataFrames by blitz
        self.filter_by_blitz()

        # Identify and sort streaks
        self.process_files()
        