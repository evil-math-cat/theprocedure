"""
This script processes chess games in PGN format, evaluates the accuracy of moves
using Stockfish, and stores the results in a CSV file.

Functionality:
--------------
- Creates a CSV file for each chess player in the format: ID, White_Accuracy, Black_Accuracy.
- Uses a previously created ID for each game to map the accuracies.
- Uses Stockfish to calculate the centipawn valuation for positions before and after each move, 
  then converts those into win percentages to compute move accuracy.
- Saves the calculated accuracies to a CSV file for further analysis.

Stockfish URL: 
    - https://github.com/official-stockfish/Stockfish/releases/latest/download/stockfish-windows-x86-64-sse41-popcnt.zip

Libraries Used:
---------------
- chess: for core chess functionalities, including board representation and move handling.
- chess.engine: to interact with the Stockfish engine.
- chess.pgn: for reading PGN files and parsing game data.
- math: for mathematical operations, especially centipawn conversion.
- os: for file and path operations.
- pandas: for data manipulation and CSV operations.

Paths:
------
- `input_stockfish`: Path to the Stockfish engine binary.
- `accuracies_paths`: Dictionary storing paths for PGN files and output CSVs for each player.
- `input_pgn`: Path to the selected player's PGN file.
- `output_accuracy_csv`: Path to the CSV file where calculated accuracies will be saved.

Main Functions:
---------------
1. load_processed_ids():
    - Loads the set of game IDs that have already been processed and saved in the CSV file.
2. save_accuracy_to_file(game_id, white_accuracy, black_accuracy):
    - Appends calculated accuracies for each game to the CSV file.
3. centipawns_to_win_percent(centipawns):
    - Converts centipawn evaluations from Stockfish into win percentages.
4. calculate_accuracy(win_percent_before, win_percent_after):
    - Calculates move accuracy based on the difference between win percentages before and after each move.
5. get_accuracy(game, game_id):
    - Computes average accuracy for white and black based on Stockfish evaluations.
6. load_last_processed_game():
    - Loads the last processed game ID from a checkpoint file.
7. save_last_processed_game(game_id):
    - Saves the last processed game ID to a checkpoint file.
8. main():
    - Main function to process all games in the PGN file, calculate accuracies, and save the results.

Usage:
------
1. Run the script.
2. Stockfish engine will analyze each move and compute the accuracy for both players.
3. Accuracies are saved to the respective CSV files for each player.
"""

import chess # core chess functionalities for board and game
import chess.engine #to interact with Stockfish
import chess.pgn # to read PGNs
import math # math ops
import os #file and path ops
import pandas as pd #data manipulation

# Path to Stockfish binary
input_stockfish = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\external\stockfish\stockfish.exe'

# Hardcode the player you want to process
selected_player = 'Hikaru Nakamura'  # Change this to the desired player

# Define accuracy paths for players
accuracies_paths = {
    'Magnus Carlsen': {
        'pgn': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\1_magnus_carlsen\3_magnus_carlsen_combined_games_sorted_with_added_headers.pgn',
        'csv': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\0_magnus_carlsen_accuracies.csv'
    },
    'Hikaru Nakamura': {
        'pgn': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\2_hikaru_nakamura\3_hikaru_nakamura_combined_games_sorted_with_added_headers.pgn',
        'csv': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\0_hikaru_nakamura_accuracies.csv'
    },
    'Fabiano Caruana': {
        'pgn': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\3_fabiano_caruana\3_fabiano_caruana_combined_games_sorted_with_added_headers.pgn',
        'csv': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\0_fabiano_caruana_accuracies.csv'
    }
}

# Paths for selected player
input_pgn = accuracies_paths[selected_player]['pgn']
output_accuracy_csv = accuracies_paths[selected_player]['csv']

def load_processed_ids():
    """ Load the IDs of the games that have already been processed. """
    if os.path.isfile(output_accuracy_csv):
        try:
            df = pd.read_csv(output_accuracy_csv)
            return set(df['ID'])
        except pd.errors.EmptyDataError:
            print(f"Warning: The file {output_accuracy_csv} is empty.")
        except Exception as e:
            print(f"Error reading {output_accuracy_csv}: {str(e)}")
    return set()

def save_accuracy_to_file(game_id, white_accuracy, black_accuracy):
    """ Save the accuracies to a CSV file """
    file_exists = os.path.isfile(output_accuracy_csv)
    
    with open(output_accuracy_csv, 'a') as f:
        # Write the header only if the file doesn't exist yet
        if not file_exists:
            f.write("ID,White Accuracy,Black Accuracy\n")
        
        # Write the accuracy data
        f.write(f"{game_id},{white_accuracy},{black_accuracy}\n")

# the following 2 functions calculate the accuracy based on Lichess.org
'''
A centipawn is a unit of measurement used in chess 
to represent the evaluation of a position by chess engines like Stockfish. 
A centipawn is equal to one hundredth of a pawn. 
In other words, 1 centipawn = 0.01 pawn.
Stockfish evaluates a position in +/- centipawns.
it considers factors like material balance, piece activity, king safety, and pawn structure, 
and provides a centipawn score for white and black.
So we will need to convert centipawns before to win % and the centipawns 
after the position to win%, so that we can then calculate the accuracy 
using the eposition before and after. Current position is valuated at something centipanws
then the position after is evaluated at something else centipawns. that's the idea.

'''
def centipawns_to_win_percent(centipawns):
    """ Convert centipawns to Win% """
    if centipawns is None:
        return 50  # Default Win% for unknown centipawns
    #formula from Lichess.org
    return 50 + 50 * (2 / (1 + math.exp(-0.00368208 * centipawns)) - 1)

def calculate_accuracy(win_percent_before, win_percent_after):
    """ Calculate Accuracy% based on the change in Win% """
    if win_percent_before is None or win_percent_after is None:
        return 0  # Default accuracy for unknown Win%
    #formula from Lichess.org
    accuracy = 103.1668 * math.exp(-0.04354 * (win_percent_before - win_percent_after)) - 3.1669
    return max(0, min(accuracy, 100))  # Clamp accuracy between 0% and 100%

# this function returns the accuracy for white and black
def get_accuracy(game, game_id): #change to get_stockfish_before_and_after_centipawn_valuation
    # Initialize the Stockfish engine, using the path to the stockfish executable.
    # then we initialize each variable. it's the game starting from scratch.
    with chess.engine.SimpleEngine.popen_uci(input_stockfish) as engine:
        board = chess.Board()
        white_accuracy_total = 0
        black_accuracy_total = 0
        white_move_count = 0
        black_move_count = 0

        '''
        UCI (Universal Chess Interface) is a communication protocol used to facilitate interaction between chess engines (software that calculates moves) and user interfaces (software that provides the visual board, handles input/output, etc.). I
        then stockfish iterates through each move, in the games moves(called mainlines)
        it will analyze each move before and after and provide centipaw valuation 
        centipawn before and centipawn after,  each of which we will then convert to %.
        % before and after and then use both in our get_accuracy to calculate the accuracy.     
        '''
        for move_number, move in enumerate(game.mainline_moves()):
            board.push(move) # this updated the move to the next position.
            
            time = 0.05 # here we give Stockfish 0.05 to evaluate each move.
            # Determine current side to move
            is_white_turn = (move_number % 2 == 0)# move index starts at 0, so white are the even positions
            
            # Analyze the position before the move using Stockfish and get its scode
            info_before = engine.analyse(board, chess.engine.Limit(time=time))
            score_before = info_before.get("score")
            '''
            The following  line retrieves the centipawn score from Stockfishs evaluation result 
            for the position before a move. It ensures that the score is only accessed 
            if its available and valid.
            Handling Missing Data: If the evaluation result is missing or 
            not properly computed, it assigns None to centipawns_before 
            to avoid errors and handle cases where evaluation data is not provided.
            '''
            centipawns_before = score_before.relative.score() if score_before and score_before.relative else None
            '''
            then we convert centipaws to % using Lichess's math
            '''
            win_percent_before = centipawns_to_win_percent(centipawns_before)
            
            # Analyze the position after the move
            info_after = engine.analyse(board, chess.engine.Limit(time=time))
            score_after = info_after.get("score")
            centipawns_after = score_after.relative.score() if score_after and score_after.relative else None
            win_percent_after = centipawns_to_win_percent(centipawns_after)

            # Calculate the accuracy of the move 
            # with the converted before and after %s of centipawns 
            accuracy = calculate_accuracy(win_percent_before, win_percent_after)

            # then we add the cumulative accuracy to the respective player
            if is_white_turn:
                white_accuracy_total += accuracy
                white_move_count += 1 # and move the piece
            else:
                black_accuracy_total += accuracy
                black_move_count += 1 # and move the piece

        # the avegave white anc black accuracy is the cumulative accuracy / total move count
        # so we can average it out.
        avg_white_accuracy = white_accuracy_total / white_move_count if white_move_count > 0 else 0
        avg_black_accuracy = black_accuracy_total / black_move_count if black_move_count > 0 else 0
        
        return avg_white_accuracy, avg_black_accuracy
    
def load_last_processed_game():
    """ Load the last processed game ID from a checkpoint file """
    checkpoint_file = output_accuracy_csv + '.checkpoint'
    if os.path.isfile(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return f.read().strip()
        except Exception as e:
            print(f"Error reading checkpoint file: {str(e)}")
    return None

def save_last_processed_game(game_id):
    """ Save the last processed game ID to a checkpoint file """
    checkpoint_file = output_accuracy_csv + '.checkpoint'
    try:
        with open(checkpoint_file, 'w') as f:
            f.write(game_id)
    except Exception as e:
        print(f"Error writing checkpoint file: {str(e)}")

def main():
    processed_ids = load_processed_ids()
    last_processed_game = load_last_processed_game()
    
    with open(input_pgn, "r", encoding='utf-8') as pgn_file:
        game_index = 0
        while True:
            game = chess.pgn.read_game(pgn_file)
            if game is None:
                break
            
            game_index += 1
            game_id = game.headers.get("ID", f"unknown_{game_index}")

            if last_processed_game and game_id <= last_processed_game:
                print(f"Skipping game ID: {game_id} (already processed)")
                continue

            if game_id in processed_ids:
                print(f"Skipping game ID: {game_id} (already processed)")
                continue

            print(f"Processing game ID: {game_id}")
            white_accuracy, black_accuracy = get_accuracy(game, game_id)
            if white_accuracy is not None and black_accuracy is not None:
                save_accuracy_to_file(game_id, white_accuracy, black_accuracy)
                save_last_processed_game(game_id)
            else:
                print(f"Failed to compute accuracies for game ID: {game_id}")

if __name__ == "__main__":
    main()