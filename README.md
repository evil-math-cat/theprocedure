# Statistical Analysis of Hikaru Nakamura's Online Games

## Introduction
This project investigates GM Hikaru Nakamura's performance in online chess, specifically addressing the accusation made by former World Chess Champion GM Vladimir Kramnik, who questioned the plausibility of Hikaru's 44.5/45 game result in a blitz series. This analysis seeks to determine whether Kramnik's concerns hold merit by performing a thorough statistical analysis.

## Features
- **Comprehensive Data Retrieval:** Automatically collects PGN files from chess.com for any player.
- **Data Processing and Cleaning:** Enhances and consolidates PGN files into player-specific datasets.
- **Data Analysis and Visualization:** Provides detailed summary statistics and visualizations using tools like DataLens.
- **Stockfish Accuracy Calculation:** Uses Stockfish to calculate move accuracies and determine overall game strength.
- **Streak and Frequency Analysis:** Analyzes streak lengths and frequencies to identify patterns and anomalies.

## Files Overview
- `app.py`: The orchestrator file that ties together all other scripts and runs the analysis.
- `data_retriever.py`: Retrieves PGN files for specified players from chess.com.
- `data_processor.py`: Enhances and concatenates PGN files, creating a comprehensive dataset for each player.
- `data_manipulator.py`: Processes the data into various CSV files, prepping for deeper analysis.
- `data_analyzer.py`: Provides summary statistics and visualizations (e.g., box plots) for players' performance.
- `stockfish_accuracies_calculator.py`: Calculates move accuracies using Stockfish for all games, with checkpointing for long-running analyses.
- `data_combinator.py`: Combines all player dataframes, categorizing streaks and their frequencies, and prepares data for dashboard uploads.

## Usage Instructions
1. Clone the repository: `git clone https://github.com/your-repo-link`
2. Install required dependencies: `pip install -r requirements.txt`
3. Run the project:
   ```bash
   python app.py
## How to Contribute
1. Fork the repository.
2. Create a new feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add your feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

## License
GPL-3.0

## Contact Information
If you have any questions, reach out:
- Email: evilmathcat@yandex.com
