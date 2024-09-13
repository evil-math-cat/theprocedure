# theprocedure
statistical analysis on the whole chess debocle. Is Kramnik right? Let's see.

# Intro
Former World Chess Champion GM Vladimir Kramnik indirectly accused GM Hikaru Nakamura, currently ranked number 2 of cheating.
He mentioned that Hikaru's performance in a series of online games, where he won 44.5/45 games is unlikely, and should be looked into.
This got me interested, so I thought it was worthwhile investigating.

My Procedure...[]TLDR]
app.py is th orchestrator file
1 - data_retriever.py   # this gets all player files from chess.com
2 - data_processor_py # this enhances the pgn files and concatenates all into one per player
3 - data_manipulator.py # this created multiple csv's to use in...
4 - data_analyzer.py ...where we can now provide summary statistics for each player and individual box_plots
# additional
5 - stockfish_accuracies_calculator.py # self explanatory. It goes through the player's games and does its thing for each. It also has checkpoints as this takes a while
6 - data_combinator.py # gets all player's dataframes and combines into one where ID is the name of the player, then Xi for Streaks and Fi for Frequencies of each streak.
