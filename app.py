from modules import DataRetriever, DataProcessor, DataManipulator

# Define the file paths and player names
# for Retriever
source_config = {
    'output_folders':{
        'magnuscarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\1_magnus_carlsen', 
        'hikaru': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\2_hikaru_nakamura',
        'fabianocaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\3_fabiano_caruana'       
    },
    'players': ['magnuscarlsen','hikaru','fabianocaruana' ]
}

# for Processor
pgn_directories = {
    r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\1_magnus_carlsen': 
    (r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\1_magnus_carlsen', '1_magnus_carlsen_combined_games.pgn'),
    r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\2_hikaru_nakamura': 
    (r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\2_hikaru_nakamura', '1_hikaru_nakamura_combined_games.pgn'),
    r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\1_raw\1_chesscom_games\3_fabiano_caruana': 
    (r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\3_fabiano_caruana', '1_fabiano_caruana_combined_games.pgn')
}

# for Manipulator
input_pgn_files = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\1_magnus_carlsen\3_magnus_carlsen_combined_games_sorted_with_added_headers.pgn',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\2_hikaru_nakamura\3_hikaru_nakamura_combined_games_sorted_with_added_headers.pgn',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\1_pgns\3_fabiano_caruana\3_fabiano_caruana_combined_games_sorted_with_added_headers.pgn'
}

output_dataframe_files = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\1_magnus_carlsen_chesscom_processed_dataframe.csv',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\1_hikaru_nakamura_chesscom_processed_dataframe.csv',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\1_fabiano_caruana_chesscom_processed_dataframe.csv'
}

output_dataframe_files_logs = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\logs\1_magnus_carlsen_chesscom_processed_dataframe_log.txt',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\logs\2_hikaru_nakamura_chesscom_processed_dataframe_log.txt',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\logs\3_fabiano_caruana_chesscom_processed_dataframe_log.txt'
}

dataframe_files_filtered_by_blitz = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\2_magnus_carlsen_chesscom_filtered_dataframe_by_blitz.csv',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\2_hikaru_nakamura_chesscom_filtered_dataframe_by_blitz.csv',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\2_fabiano_caruana_chesscom_filtered_dataframe_by_blitz.csv'
}

processed_streaks_unordered = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\3a_magnus_carlsen_chesscom_processed_streaks_unordered.csv',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\3a_hikaru_nakamura_chesscom_processed_streaks_unordered.csv',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\3a_fabiano_caruana_chesscom_processed_streaks_unordered.csv'
}

processed_streaks_ordered = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\3b_magnus_carlsen_chesscom_processed_streaks_ordered_version.csv',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\3b_hikaru_nakamura_chesscom_processed_streaks_ordered_version.csv',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\3b_fabiano_caruana_chesscom_processed_streaks_ordered_version.csv'
}

processed_details = {
    'MagnusCarlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\1_magnus_carlsen\4_magnus_carlsen_chesscom_processed_streaks_details.csv',
    'HikaruNakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\2_hikaru_nakamura\4_hikaru_nakamura_chesscom_processed_streaks_details.csv',
    'FabianoCaruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\2_processed\2_csvs\3_fabiano_caruana\4_fabiano_caruana_chesscom_processed_streaks_details.csv'
}

winner_names = {
    'MagnusCarlsen': 'MagnusCarlsen',
    'HikaruNakamura': 'Hikaru',
    'FabianoCaruana': 'FabianoCaruana'
}

def main(): 
    
    # Initializer DataRetriever
    retriever = DataRetriever(source_config)
    retriever.process_data()
    
    # Initialize DataProcessor with pgn directories
    processor = DataProcessor(pgn_directories)
    processor.process_all_pgn_files()
        
    # Initialize DataManipulator with the dataframes and file paths

    manipulator = DataManipulator(input_pgn_files, output_dataframe_files, 
                                output_dataframe_files_logs, 
                                dataframe_files_filtered_by_blitz,
                                processed_streaks_unordered, 
                                processed_streaks_ordered, 
                                processed_details, winner_names)    

    manipulator.process_dataframes()

       
if __name__ == "__main__":
    main()