import pandas as pd

# Define the file paths
magnus_path = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\MagnusCarlsen_frequencies.csv'
hikaru_path = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\HikaruNakamura_frequencies.csv'
fabiano_path = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\FabianoCaruana_frequencies.csv'
output_path = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\combined_frequencies2.csv'

# Load each CSV file
df_magnus = pd.read_csv(magnus_path)
df_hikaru = pd.read_csv(hikaru_path)
df_fabiano = pd.read_csv(fabiano_path)

# Add ID column
df_magnus['ID'] = 'Magnus Carlsen'
df_hikaru['ID'] = 'Hikaru Nakamura'
df_fabiano['ID'] = 'Fabiano Caruana'

# Combine all DataFrames
# ignore_index=True: Resets the index when combining DataFrames.
combined_df = pd.concat([df_magnus, df_hikaru, df_fabiano], ignore_index=True)

# Rearrange the columns to match the desired format (ID, Xi, Fi)
combined_df = combined_df[['ID', 'Xi', 'Fi']]

# Save to a new CSV file
# index=False: Prevents the index from being saved to a CSV file, 
# results in a cleaner output.
combined_df.to_csv(output_path, index=False)
