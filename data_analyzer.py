import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# File paths
input_files = {
    'Magnus Carlsen': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\MagnusCarlsen_frequencies.csv',
    'Hikaru Nakamura': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\HikaruNakamura_frequencies.csv',
    'Fabiano Caruana': r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization\FabianoCaruana_frequencies.csv'
}

# Output folder
output_folder = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\3_visualization'

# Function to calculate summary statistics and save plot
def summary_statistics(df, player_name):
    X = df['Xi']  # Streaks (values)
    F = df['Fi']  # Frequencies
    
    # Repeat each streak according to its frequency to create the full dataset
    full_data = np.repeat(X, F)
    
    # Calculate statistics
    mean = round(np.mean(full_data), 2)
    median = round(np.median(full_data), 2)
    mode = X[F.idxmax()]  # The value with the highest frequency
    p1 = round(np.percentile(full_data, 1), 2)
    p5 = round(np.percentile(full_data, 5), 2)
    Q1 = round(np.percentile(full_data, 25), 2)
    Q2 = round(np.percentile(full_data, 50), 2)  # This is the same as the median
    Q3 = round(np.percentile(full_data, 75), 2)
    p99 = round(np.percentile(full_data, 99), 2)
    highest_streak = np.max(X)
    freq_of_highest_streak = F[X.idxmax()]
    
    # Print summary statistics
    print(f"\nSummary Statistics for {player_name}:")
    print(f"Mean: {mean}")
    print(f"Median: {median}")
    print(f"Mode: {mode}")
    print(f"P1: {p1}")
    print(f"P5: {p5}")
    print(f"Q1: {Q1}")
    print(f"Q2 (Median): {Q2}")
    print(f"Q3: {Q3}")
    print(f"P99: {p99}")
    print(f"Highest streak found: {highest_streak}")
    print(f"Frequency of the highest streak: {freq_of_highest_streak}")
    
    # Create a boxplot and extract the outliers
    fig, ax = plt.subplots(figsize=(10, 6))  # Increase figure size
    box = ax.boxplot(full_data, vert=False, patch_artist=True, flierprops=dict(markerfacecolor='r', marker='o'))
    
    # Extract the outliers
    outliers = box['fliers'][0].get_xdata()
    if len(outliers) > 0:
        lowest_outlier = min(outliers)
        largest_outlier = max(outliers)
        
        # Find the corresponding Xi values for the outliers
        lowest_outlier_xi = X[np.abs(X - lowest_outlier).argmin()]
        largest_outlier_xi = X[np.abs(X - largest_outlier).argmin()]
    else:
        lowest_outlier_xi = largest_outlier_xi = 'None'
    
    # Create the legend text
    legend_text = (f"Q1: {Q1:.2f}\nMedian (Q2): {Q2:.2f}\nMean: {mean:.2f}\nQ3: {Q3:.2f}\n"
                   f"Lowest Outlier: {lowest_outlier_xi}\nLargest Outlier: {largest_outlier_xi}")
    
    # Add summary statistics on the plot
    plt.text(0.95, 0.95, legend_text, transform=ax.transAxes, fontsize=10,
             verticalalignment='top', horizontalalignment='right', bbox=dict(facecolor='white', alpha=0.8))
    
    # Set the title and labels
    plt.title(f'Box Plot for {player_name}', fontsize=14)
    plt.xlabel('Streaks', fontsize=12)
    
    # Save the plot
    output_file = os.path.join(output_folder, f"{player_name.replace(' ', '')}_boxplot.jpg")
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as {output_file}")
    
    # Close the plot to free up memory
    plt.close(fig)

# Loop through each dataset and calculate summary statistics
for player_name, file_path in input_files.items():
    # Read CSV into a DataFrame
    df = pd.read_csv(file_path)
    
    # Ensure the columns are named 'Xi' (values) and 'Fi' (frequencies)
    assert 'Xi' in df.columns and 'Fi' in df.columns, "Columns 'Xi' and 'Fi' must be present in the dataset."
    
    # Calculate summary statistics and plot
    summary_statistics(df, player_name)

print("All plots have been saved.")