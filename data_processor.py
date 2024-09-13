# actual data_processor file

"""
A class to process and analyze chess PGN (Portable Game Notation) files. 
This includes combining, sorting, adding additional headers, 
and analyzing missing data such as time control.

Attributes:
-----------
pgn_directories : dict
    A dictionary where keys are input directories and values 
    are tuples with output directories and filenames.

Methods:
--------
concatenate_pgn_files(input_directory: str, output_directory: str, output_filename: str):
    Combines all PGN files from the input directory into one file 
    and saves it in the output directory.

sort_pgn_file(input_file: str, output_file: str):
    Sorts a combined PGN file based on the UTCDate and UTCTime headers.

parse_game(game: str) -> tuple:
    Extracts the date and time from a PGN game to facilitate sorting.

get_place(site_header: str, event_header: str, link_header: str = "") -> str:
    Determines whether a game was played online or offline 
    based on keywords in the site, event, or link headers.

get_time_control(event_header: str, time_control_header: str) -> str:
    Determines the time control type (daily, blitz, rapid, classical) 
    based on the event and time control headers.

add_headers_to_pgn(input_pgn: str, output_pgn: str):
    Adds headers such as ID, Place, and Time_Control to each game in a PGN file.

analyze_pgn_for_missing_timecontrol(input_file: str, output_csv: str) -> int:
    Analyzes a PGN file for missing Time_Control headers and 
    logs the games missing this data to a CSV file.

extract_unique_events(file_paths: list) -> list:
    Extracts unique events from a list of CSV files and returns them as a sorted list.

process_all_pgn_files():
    Orchestrates the full PGN processing pipeline: combining, sorting, 
    adding headers, analyzing for missing
    Time_Control data, and extracting unique events.
"""

import os
import glob
import re
from datetime import datetime
import chess.pgn
import csv
import pandas as pd
from typing import Union, Dict

'''
about self:
In Python, self refers to the instance of the class. 
It is used inside class methods to access instance attributes and other methods. 
By passing self as the first argument, you can modify the object's state and call its methods. 
Without self, the method wouldn't know which instance's data to use.
it essentially means do stuff with this object.
'''
class DataProcessor:
    def __init__(self, pgn_directories):
        self.pgn_directories = pgn_directories

    def concatenate_pgn_files(self, input_directory, output_directory, output_filename):
        # Create the output directory if it doesn't exist
        # if it does exist do nothing.
        os.makedirs(output_directory, exist_ok=True)
        
        # Get all .pgn files in the input directory
        pgn_files = glob.glob(os.path.join(input_directory, '*.pgn'))
        
        # Sort the files based on their names (which are in YYYY-MM format)
        # just to make sure.
        pgn_files.sort()
        
        # Define our output path for the file,
        # passing in as parameters, the output directory and the output file name
        output_path = os.path.join(output_directory, output_filename)
        
        # Open the output file in write mode and concatenate files into one
        with open(output_path, 'w', encoding='utf-8') as outfile: # outfile is an object
            # Iterate through sorted files
            for pgn_file in pgn_files:
                with open(pgn_file, 'r', encoding='utf-8') as infile:
                    # Read the content of each file and write it to the output file
                    outfile.write(infile.read())
                    # Add a newline between files to ensure separation
                    outfile.write('\n')

    def parse_game(self, game):
        # search for time and date the pattern in group(1) ->the game
        # [UTCDate "2014.01.07"]
        # [UTCTime "03:56:00"]
        date_match = re.search(r'\[UTCDate "(\d{4}\.\d{2}\.\d{2})"\]', game)
        time_match = re.search(r'\[UTCTime "(\d{2}:\d{2}:\d{2})"\]', game)
        
        if date_match and time_match:
            date_str = date_match.group(1).replace('.', '-')
            time_str = time_match.group(1)
            # if both are found, return a tuple(, ) with the parsed date and time and the game
            # we need the date in this format ...-... to be able to sort it.
            return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S"), game
        # if no match is found, returns none for the date and just returns the game.
        return None, game

    def sort_pgn_file(self, input_file, output_file):
        # Read the concatenated PGN file
        with open(input_file, 'r', encoding='utf-8') as infile:
            content = infile.read()

        # Regular expression to match all PGN games
        # in regex this (?=...)  is the expression we use to look for something after the =
        # then since we want [Event  so that we can include the [ in the search regex
        # requires we do \ so what we are saying here is:
        # split blank lines and [Event into the games list of strings.
        games = re.split(r'\n\n(?=\[Event )', content)
        
        # Sort the games based on date and time
        sorted_games = sorted(
            (self.parse_game(game) for game in games),
            # this is the sorting key. 
            # if the first game date and time couldn't be parsed, 
            # datetime.min is used as a sorting key.
            # so games with missing dates/times go to the beginning.
            key=lambda x: x[0] or datetime.min
        )
        
        # Write the sorted games to the output file
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # remembe that from parsed games it does: return None, game
            # if no match is found, returns none for the date and just returns the game.
            # we are only interested in the games.
            for _, game in sorted_games:
                outfile.write(game + '\n\n')
        
        print(f"\nSorted PGN file created: {output_file}")

    def get_place(self, site_header, event_header, link_header=""):
        # Define the keywords for online sites
        online_sites = ['chess.com', 'chess24.com', 'lichess.org', 'chess24']
        online_events = ['titled tuesday', 'early', 'late', 'main event', 'Play-in', 'Match Play']
        
        # Convert headers to lowercase for case-insensitive comparison
        site_header = site_header.lower()
        event_header = event_header.lower()
        link_header = link_header.lower()
        
        # Check if the link header contains "daily"
        if "daily" in link_header:
            return "Online"  # Assuming daily games are played online

        # Check if the site header contains any of the online site keywords
        if any(site in site_header for site in online_sites):
            return 'Online'
        
        # Check if the event header contains any of the online event keywords
        if any(event in event_header for event in online_events):
            return 'Online'
        
        return 'Offline'

    def get_time_control(self, event_header, time_control_header):
        # Convert the event header to lowercase for case-insensitive matching
        event_header = event_header.lower()

        # Dictionary mapping for event keywords to their respective time controls
        event_mapping = {
            "le troph?e ccas, final":"rapid",
            "corus group a": "classical",
            "january": "blitz",
            "february": "blitz",
            "march": "blitz",
            "april":"blitz",
            "may": "blitz",
            "june": "blitz",
            "july": "blitz",
            "august": "blitz",
            "september": "blitz",
            "october": "blitz",
            "november": "blitz",
            "december": "blitz",
            "121. ch-new york state": "classical",
            "121st ch-new york state": "classical",
            "17. euro team ch (m)": "classical",
            "2010 u.s. championship - final quad": "classical",
            "2019-grand-chess-tour-superbet": "classical",
            "2019-grand-chess-tour-tata-steel": "classical",
            "2019-london-chess-classic": "classical",
            "2020-candidates-chess-tournament": "classical",
            "2021-candidates-chess-tournament": "classical",
            "3. euicc": "classical",
            "37. national chess congress": "classical",
            "4. young stars of the world": "classical",
            "53. reggio emilia": "classical",
            "54 torneo di capodanno": "classical",
            "april 16 early 2024": "blitz",
            "april 16 late 2024": "blitz",
            "april 2 early 2024": "blitz",
            "april 2 late 2024": "blitz",
            "april 23 early 2024": "blitz",
            "april 23 late 2024": "blitz",
            "april 30 early 2024": "blitz",
            "april 30 late 2024": "blitz",
            "april 9 late 2024": "blitz",
            "august 06 early 2024": "blitz",
            "august 06 late 2024": "blitz",
            "august 13 early 2024": "blitz",
            "august 13 late 2024": "blitz",
            "august 20 early 2024": "blitz",
            "august 20 late 2024": "blitz",
            "bazna king's tournament": "classical",
            "bullet chess championship 2021": "bullet",
            "cct qualifier 14 2023": "rapid",
            "candidates": "classical",
            "ch  amateur team (east)": "classical",
            "ch amateur team": "classical",
            "ch amateur team (east)": "classical",
            "ch barents region a": "classical",
            "ch chess club": "classical",
            "ch eastern class": "classical",
            "ch europe": "classical",
            "ch europe (juniors) (under 12)": "classical",
            "ch europe (juniors) (under 14)": "classical",
            "ch madrid (final)": "classical",
            "ch manhattan chess club": "classical",
            "ch new york state": "classical",
            "ch norway": "classical",
            "ch norway (play-off)": "classical",
            "ch norway (team)": "classical",
            "ch norway (team) (final)": "classical",
            "ch norway (team) 2003/04 (final)": "classical",
            "ch pan-american (juniors) (under 20)": "classical",
            "ch pan-american continental": "classical",
            "ch spain (team)": "classical",
            "ch state new york": "classical",
            "ch usa": "classical",
            "ch usa (gr a)": "classical",
            "ch usa (amateur) (team) east": "classical",
            "ch usa (play-off)": "classical",
            "ch world (juniors) (under 10)": "classical",
            "ch world (juniors) (under 12)": "classical",
            "ch world (juniors) (under 14)": "classical",
            "champions chess960 g30": "rapid",
            "chess.com global championship finals 2022": "classical",
            "classic rr": "classical",
            "corus group a": "classical",
            "cup european club (final)": "classical",
            "cup fibertex (blindfold)": "rapid",
            "cup world fide": "classical",
            "cup world fide (active)": "rapid",
            "cup world fide (places 9-10)": "classical",
            "cup world fide (places 9-12)": "classical",
            "cup world fide (places 9-16)": "classical",
            "december 12 late 2023": "blitz",
            "december 19 early 2023": "blitz",
            "december 19 late 2023": "blitz",
            "december 26 early 2023": "blitz",
            "december 26 late 2023": "blitz",
            "december 5 early 2023": "blitz",
            "december 5 late 2023": "blitz",
            "division i": "classical",
            "division i placement": "classical",
            "division ii": "classical",
            "dos hermanas qualifier (14-a)": "blitz",
            "dos hermanas qualifier (15-a)": "blitz",
            "dos hermanas qualifier (15-b)": "blitz",
            "dos hermanas qualifier (16-a)": "blitz",
            "dos hermanas qualifier (17-a)": "blitz",
            "dos hermanas qualifier (final)": "blitz",
            "european chess club cup 2022": "classical",
            "european club cup 2021": "classical",
            "fide world championship 2021": "classical",
            "fide world chess championship 2021": "classical",
            "february 07 late 2023": "blitz",
            "february 13 early 2024": "blitz",
            "february 13 late 2024": "blitz",
            "february 14 early 2023": "blitz",
            "february 14 late 2023": "blitz",
            "february 20 early 2024": "blitz",
            "february 20 late 2024": "blitz",
            "february 21 early 2023": "blitz",
            "february 21 late 2023": "blitz",
            "february 27 early 2024": "blitz",
            "february 27 late 2024": "blitz",
            "february 28 early 2023": "blitz",
            "february 28 late 2023": "blitz",
            "february 6 early 2024": "blitz",
            "february 6 late 2024": "blitz",
            "january 16 early 2024": "blitz",
            "january 16 late 2024": "blitz",
            "january 2 early 2024": "blitz",
            "january 2 late 2024": "blitz",
            "january 23 early 2024": "blitz",
            "january 23 late 2024": "blitz",
            "january 30 early 2024": "blitz",
            "january 30 late 2024": "blitz",
            "january 31 early 2023": "blitz",
            "january 31 late 2023": "blitz",
            "january 9 early 2024": "blitz",
            "january 9 late 2024": "blitz",
            "july 02 early 2024": "blitz",
            "july 02 late 2024": "blitz",
            "july 09 early 2024": "blitz",
            "july 09 late 2024": "blitz",
            "july 16 early 2024": "blitz",
            "july 16 late 2024": "blitz",
            "july 23 early 2024": "blitz",
            "july 23 late 2024": "blitz",
            "july 30 early 2024": "blitz",
            "july 30 late 2024": "blitz",
            "june 11 early 2024": "blitz",
            "june 11 late 2024": "blitz",
            "june 18 early 2024": "blitz",
            "june 18 late 2024": "blitz",
            "june 25 early 2024": "blitz",
            "june 25 late 2024": "blitz",
            "june 4 late 2024": "blitz",
            "le trophée ccas, final": "classical",
            "lorca esp, narciso yepes mem 05": "classical",
            "los inmortales": "classical",
            "main": "classical",
            "main event": "classical",
            "manhattan cc": "classical",
            "march 07 early 2023": "blitz",
            "march 07 late 2023": "blitz",
            "march 12 early 2024": "blitz",
            "march 12 late 2024": "blitz",
            "march 19 early 2024": "blitz",
            "march 19 late 2024": "blitz",
            "march 26 early 2024": "blitz",
            "march 26 late 2024": "blitz",
            "march 5 early 2024": "blitz",
            "march 5 late 2024": "blitz",
            "may 14 early 2024": "blitz",
            "may 14 late 2024": "blitz",
            "may 21 early 2024": "blitz",
            "may 21 late 2024": "blitz",
            "may 28 late 2024": "blitz",
            "may 7 early 2024": "blitz",
            "may 7 late 2024": "blitz",
            "meltwater champions chess tour finals": "rapid",
            "meltwater champions chess tour | julius baer generation cup | prelims": "rapid",
            "meltwater champions chess tour | major 2 | prelims": "rapid",
            "memorial claude pecaut (cat.7)": "classical",
            "memorial elekes (b)": "classical",
            "nor ch": "classical",
            "november 21 early 2023": "blitz",
            "november 21 late 2023": "blitz",
            "november 28 early 2023": "blitz",
            "november 28 late 2023": "blitz",
            "play-in": "rapid",
            "pro chess league main event 2023 regular season": "rapid",
            "qualifier 1 swiss": "blitz",
            "qualifier 2 swiss": "blitz",
            "round robin stage": "classical",
            "scc grand prix 1 2021": "blitz",
            "scc grand prix 4 2021": "blitz",
            "speed chess championship main event 2021": "blitz",
            "speed chess championship main event 2022": "blitz",
            "speed chess championship super swiss 2021": "blitz",
            "survival stage": "blitz",
            "tournament \\": "classical",
            "trial 1 brought to you by airbnb": "classical",
            "trial 3 brought to you by alianz": "classical",
            "u.s. chess championship 2021": "classical",
            "u.s. chess championship 2022": "classical",
            "u.s. chess championship 2023": "classical",
            "world championship candidates": "classical",
            "world chess championship carlsen vs. caruana 2018": "classical",
            "trophee ccas gpb": "classical",
            "european teams": "classical",
            "final masters": "classical",
            "chess.com speed 5m+2spm": "blitz",
            "vi world blitz": "blitz",
            "norway chess": "classical",
            "masters final": "classical",
            "corus b": "classical",
            "wyb": "blitz",
            "saint louis blitz": "blitz",
            "olympiad men": "classical",
            "sinquefield cup": "classical",
            "titled tue": "blitz",
            "carlsen vs chess.com": "blitz",
            "chess.com sf blitz 5m+2spm": "blitz",
            "titled arena": "blitz",
            "ecc": "classical",
            "charity cup ko": "classical",
            "norway armageddon": "blitz",
            "play live challenge": "blitz",
            "wcc": "classical",
            "london classic tb": "classical",
            "aker cc blitz": "blitz",
            "chessable masters div 1 l": "rapid",
            "essent crown": "classical",
            "sinquefield cup tb": "classical",
            "norway blitz": "blitz",
            "goldmoney asian prelim": "classical",
            "chess.com classic div 1 w": "classical",
            "glitnir blitz prelim": "blitz",
            "tch-nor finals": "classical",
            "world blitz": "blitz",
            "tal memorial blitz": "blitz",
            "superbet rapid": "rapid",
            "wch rapid tb": "rapid",
            "tch-nor": "classical",
            "chess.com blitz final 3m+2spm": "blitz",
            "bnbank blitz gpa": "blitz",
            "world rapid": "rapid",
            "fide chess.com grand swiss": "classical",
            "chess classic": "classical",
            "ym": "classical",
            "mrdodgy inv 3 gpa": "classical",
            "world blitz final": "blitz",
            "london classic 2015": "classical",
            "zurich cc blitz": "blitz",
            "chess.com speed": "blitz",
            "ch-nor rapid playoff": "rapid",
            "wch": "classical",
            "supergm": "classical",
            "ch-nor playoff": "classical",
            "tch-nor elite": "classical",
            "arctic stars final": "classical",
            "superbet pol blitz": "blitz",
            "uva rapid open": "rapid",
            "aimchess rapid ko": "rapid",
            "smartfish masters": "rapid",
            "gct rapid tb paris": "rapid",
            "tata steel gpa": "classical",
            "magnus vs. the net": "classical",
            "nrk carlsen vs norway tv": "classical",
            "aimchess rapid div 1 w": "rapid",
            "skilling open ko": "rapid",
            "uva four player ko": "classical",
            "grenkeleasing rapid wch": "rapid",
            "legends of chess prelim": "rapid",
            "classics gm": "classical",
            "global chess league": "classical",
            "etcc": "classical",
            "airthings masters div 1 w": "rapid",
            "tata steel gma": "classical",
            "vugar gashimov mem": "classical",
            "clutch chess showdown int": "blitz",
            "tal memorial": "classical",
            "london classic": "classical",
            "sparkassen gm open": "classical",
            "tch-nor open": "classical",
            "chess24 banter final": "blitz",
            "tata steel india rapid": "rapid",
            "croatia gct": "classical",
            "grenke chess classic": "classical",
            "unam carlsen-mundo": "classical",
            "carlsen inv final": "classical",
            "carlsen-tang bullet": "blitz",
            "fide candidates": "classical",
            "pearl spring": "classical",
            "dm carlsen-artemiev": "classical",
            "corus a": "classical",
            "olympiad": "classical",
            "rapid ko": "rapid",
            "chess.com iom masters": "classical",
            "chessable masters div 1 w": "classical",
            "arctic stars prelim": "blitz",
            "airthings masters ko": "rapid",
            "5th mtel masters": "classical",
            "chess.com blitz final 5m+2spm": "blitz",
            "tata steel gpa": "classical",
            "london chess classic": "classical",
            "nic classic ko": "classical",
            "gct blitz yournextmove": "blitz",
            "nic classic prelim": "classical",
            "politiken cup": "classical",
            "san fermin masters final": "classical",
            "mrdodgy inv 3 finals": "classical",
            "midnight sun": "classical",
            "chessable masters prelim": "classical",
            "mikhail tal memorial": "classical",
            "superunited blitz": "blitz",
            "ecc open": "classical",
            "glitnir blitz": "blitz",
            "casablanca chess": "classical",
            "trophee ccas ko": "classical",
            "tata steel masters": "classical",
            "grenke classic tb": "classical",
            "fide world cup": "classical",
            "norway supreme masters blitz": "blitz",
            "airthings masters prelim ko": "rapid",
            "chess.com qf blitz 3m+2spm": "blitz",
            "wcc places 9-16": "classical",
            "chess.com speedches": "blitz",
            "magnus carlsen inv ko": "classical",
            "arctic chess challenge": "classical",
            "ai cup div 1 w": "classical",
            "amber blindfold": "classical",
            "superunited rapid": "rapid",
            "magnus carlsen inv prelim": "classical",
            "amber rapid": "rapid",
            "chessable masters gpa": "classical",
            "chess.com qf blitz 5m+2spm": "blitz",
            "eyb": "classical",
            "nordic championships": "classical",
            "open": "classical",
            "tch-nor ostland div 1": "classical",
            "aimchess us rapid ko": "rapid",
            "aker cc rapid": "rapid",
            "blitz": "blitz",
            "four player festa da uva extra": "classical",
            "carlsen-ding showdown g5": "blitz",
            "chess24 banter blitz cup": "blitz",
            "tch-esp": "classical",
            "qatar masters tb": "classical",
            "chess.com sf blitz 1m+1spm": "blitz",
            "gct blitz paris": "blitz",
            "world rapid final": "rapid",
            "vg carlsen vs norge": "classical",
            "aimchess us rapid prelim": "rapid",
            "bnbank blitz ko": "blitz",
            "hermanas internet": "blitz",
            "biel accentus gm": "classical",
            "bundesliga": "classical",
            "fibertex cup blindfold": "classical",
            "grenke chess playoffs": "classical",
            "dm carlsen-dubov": "classical",
            "nh hotels": "classical",
            "gm classic": "classical",
            "chess.com speed 3m+2spm": "blitz",
            "cct final playoff": "classical",
            "biel exhibition blitz": "blitz",
            "pro league group stage": "classical",
            "oibm": "classical",
            "fide world blitz": "blitz",
            "chess.com speedchess": "blitz",
            "fsgm": "classical",
            "lindores abbey final": "classical",
            "raw world chess challenge": "classical",
            "superbet blitz": "blitz",
            "gpb": "classical",
            "meltwater tour final": "rapid",
            "ftx crypto cup": "rapid",
            "gct rapid paris": "rapid",
            "grand slam final": "classical",
            "tch-nor final": "classical",
            "ch-barents region a": "classical",
            "icc open final": "classical",
            "botvinnik memorial": "classical",
            "gmc": "classical",
            "open nor-ch": "classical",
            "6th eicc": "classical",
            "chess classic gm": "classical",
            "3rd norway blitz": "blitz",
            "carlsen tour final": "classical",
            "cct final ko": "classical",
            "bosnia gm": "classical",
            "saint louis rapid": "rapid",
            "faaborg midtfyn cup blindfold final": "classical",
            "9th sinquefield cup": "classical",
            "supreme masters": "classical",
            "pro league ko stage": "blitz",
            "chessable masters final": "classical",
            "goldmoney asian rapid ko": "rapid",
            "julius baer gencup prelim": "rapid",
            "cuadrangular unam": "classical",
            "ch-nor": "rapid",
            "world blitz tb": "blitz",
            "wyb": "classical",
            "katara bullet final": "blitz",
            "tata steel india blitz": "blitz",
            "aeroflot open": "classical",
            "bundesliga": "rapid",
            "solidarity nor-ukr match": "classical",
            "qatar masters open": "classical",
            "opera euro rapid prelim": "rapid",
            "kings tournament": "classical",
            "chess.com speedchess q2sw": "blitz",
            "legends of chess final": "rapid",
            "world cup": "classical",
            "carlsen-ding showdown": "rapid",
            "julius baer rapid": "rapid",
            "biel gm": "classical",
            "aimchess rapid prelim": "rapid",
            "classics ima": "classical",
            "olympiad open": "classical",
            "sigeman & co": "classical",
            "bullet chess winners": "blitz",
            "aimchess rapid play-in": "rapid",
            "tch-nor qualifier": "rapid",
            "match of the hopes": "classical",
            "fide gp": "classical",
            "carlsen-ding showdown g10": "blitz",
            "aosta open": "classical",
            "wcc places": "classical",
            "grand slam final masters": "classical",
            "rapid": "rapid",
            "tch-nor kvalifisering eliteserien": "rapid",
            "skilling open prelim": "rapid",
            "fide world rapid": "rapid",
            "blindfold world cup": "classical",
            "4th kings tournament": "classical",
            "rapid match": "rapid",
            "chess.com blitz final 1m+1spm": "blitz",
            "oslo esports cup": "rapid",
            "samba cup": "rapid",
            "charity cup prelim": "rapid",
            "aerosvit": "classical",
            "fide steinitz mem open": "classical",
            "magnus-alireza": "classical",
            "final masters blitz tie-break": "blitz",
            "chess.com speed 1m+1spm": "blitz",
            "14-board simul": "classical",
            "faaborg midtfyn blindfold cup": "classical",
            "zurich cc rapid": "rapid",
            "julius baer gencup ko": "rapid",
            "zurich chess challenge": "rapid",
            "final master playoff": "blitz",
            "ftx crypto cup prelim": "rapid",
            "carlsen vs. challengers": "classical",
            "gm blitz playoff": "blitz",
            "airthings masters": "rapid",
            "tch-nor elite": "rapid",
            "superunited blitz": "blitz",
            "festival gm": "classical",
            "ata steel gpa": "classical",
            "magnus-alireza bullet": "blitz",
            "chess.com qf blitz 1m+1spm": "blitz",
            "four player festa da uva": "classical",
            "lindores abbey stars": "classical",
            "chess.com sf blitz 3m+2spm": "blitz",
            "aimchess play-in match": "rapid",
            "claude pecaut mem": "classical",
            "wch candidates s/f": "classical",
            "carlsen-ding showdown g20": "rapid",
            "dsb match": "classical",
            "carlsen vienna blindfold": "classical",
            "world corporate east d": "classical",
            "opera euro rapid ko": "rapid",
            "fide wch ko": "classical",
            "dsb match blitz": "blitz",
            "aker cc rapid final": "rapid",
            "tal mem blitz": "blitz",
            "magnus inv simul": "classical",
            "troll masters": "classical",
            "iv shakkinet a": "classical",
            "superbet pol rapid": "rapid",
            "world blitz": "blitz",
            "gm": "classical",
            "ftx crypto cup ko": "rapid",
            "bullet chess losers": "blitz",
            "pro league prelim": "rapid",
            "gct rapid yournextmove": "rapid",
            "tata steel": "rapid",
            "gma": "classical",
            "grenkeleasing rapid wch final": "rapid",
            "chessable masters ko 2022": "rapid",
            "carlsen inv prelim": "rapid",
            "tal mem": "classical",
            "ecc men": "classical",
            "lindores abbey prelim": "rapid",
            "bygger'n masters": "classical",
            "ko": "rapid",
            "world op": "classical",
            "cardoza us op": "classical",
            "eastern op": "classical",
            "new york state-ch": "classical",
            "us-cht amateur east": "classical",
            "caribbean op": "classical",
            "new york state-ch 121st": "classical",
            "manhattan cc-ch": "classical",
            "kasparov cadet gp": "classical",
            "fsimb june": "rapid",
            "new york state ch": "classical",
            "us-china summit": "classical",
            "mayor's cup": "classical",
            "smartchess.com it": "rapid",
            "us amateur team east": "classical",
            "panam u20": "classical",
            "2nd china-usa summit": "classical",
            "imre konig mem": "classical",
            "los inmortales iv": "classical",
            "ch-usa": "classical",
            "60th ny masters": "classical",
            "63rd ny masters": "classical",
            "64th ny masters": "classical",
            "ii american continental": "classical",
            "levy mem": "classical",
            "79th ny masters": "classical",
            "81st ny masters": "classical",
            "it": "classical",
            "91st ny masters": "classical",
            "v millennium festival": "classical",
            "102nd ny masters": "classical",
            "112th ny masters": "classical",
            "113th ny masters": "classical",
            "state-ch": "classical",
            "117th ny masters": "classical",
            "118th ny masters": "classical",
            "monarch assurance": "classical",
            "121st ny masters": "classical",
            "124th ny masters": "classical",
            "125th ny masters": "classical",
            "match": "classical",
            "127th ny masters": "classical",
            "gibraltar masters": "classical",
            "128th ny masters": "classical",
            "hb global cc": "classical",
            "it a": "classical",
            "young masters": "classical",
            "ch-usa gpa": "classical",
            "37th national chess congress": "classical",
            "gibtelecom masters": "classical",
            "129th ch-new york state": "classical",
            "uscl 2007": "classical",
            "casino": "classical",
            "corsica masters": "classical",
            "uscl wildcard 2007": "classical",
            "uscl semi-finals 2007": "classical",
            "6th gibtelecom masters": "classical",
            "tch-fra top 16 gp a": "classical",
            "tch-fra top 16 poule basse": "classical",
            "9th tim": "classical",
            "uscl 2008": "classical",
            "uscl ko 2008": "classical",
            "7th gibtelecom masters": "classical",
            "top 16 gpa": "classical",
            "top 16 poule haute": "classical",
            "3rd nh chess tournament": "classical",
            "uscl 2009": "classical",
            "uscl qf 2009": "classical",
            "7th world team championship": "classical",
            "ch-usa quads": "classical",
            "living chess": "classical",
            "4th nh chess tournament": "classical",
            "5th nh chess tournament": "classical",
            "5th nh chess tournament playoff": "classical",
            "uscl": "classical",
            "trophee ccas gpa": "classical",
            "bnbank final ko": "rapid",
            "bnbank gp1": "rapid",
            "nakamura-ponomariov classical match": "classical",
            "us chess league 2011": "classical",
            "54th reggio emilia": "classical",
            "tch-ita 2012": "classical",
            "ch-usa 2012": "classical",
            "28th european club cup": "classical",
            "16th unive crown": "classical",
            "sportaccord blindfold men 2012": "classical",
            "makedonia palace gp": "classical",
            "geneva masters ko 2013": "rapid",
            "world teams 2013": "classical",
            "5th classic gpc 2013": "classical",
            "5th classic ko 2013": "rapid",
            "46th tch-ita 2014": "classical",
            "cez trophy 2014": "classical",
            "baku fide grand prix 2014": "classical",
            "nakamura-aronian m 2014": "classical",
            "6th lcc pro-biz cup 2014": "classical",
            "gibraltar masters 2015": "classical",
            "4th zurich cc classical": "classical",
            "4th zurich cc tb": "blitz",
            "ch-usa 2015": "classical",
            "47th italian teams 2015": "classical",
            "millionaire chess op 2015": "classical",
            "millionaire tb b m 2015": "blitz",
            "millionaire tb final 2015": "blitz",
            "millionaire tb b 2015": "blitz",
            "millionaire chess ko 2015": "rapid",
            "showdown basque 2015": "classical",
            "gibraltar masters 2016": "classical",
            "gibraltar masters tb": "blitz",
            "5th zurich cc 2016": "classical",
            "ch-usa 2016": "classical",
            "champions showdown 60m": "rapid",
            "pro league atlantic 2017": "rapid",
            "gibraltar masters 2017": "classical",
            "sharjah grand prix 2017": "classical",
            "ch-usa 2017": "classical",
            "zurich korchnoi cc 2017": "classical",
            "moscow grand prix 2017": "classical",
            "champions showdown g30": "rapid",
            "champions showdown g20": "rapid",
            "champions showdown g10": "blitz",
            "champions showdown g5": "blitz",
            "palma de mallorca gp 2017": "classical",
            "gibraltar masters 2018": "classical",
            "gibraltar masters tb 2018": "blitz",
            "ch-usa 2018": "classical",
            "pro league all stars st1": "rapid",
            "pro league all stars st2": "rapid",
            "gibraltar masters 2019": "classical",
            "63rd ch-usa 2019": "classical",
            "moscow fide grand prix": "classical",
            "riga fide grand prix 2019": "classical",
            "hamburg fide grand prix": "classical",
            "online nations cup prelim": "rapid",
            "online nations cup final": "rapid",
            "clutch champions showdown": "rapid",
            "speed chess super swiss": "blitz",
            "speed super swiss ko 2020": "blitz",
            "ch-usa 2020": "classical",
            "chess.com sig bullet ko": "blitz",
            "chessable masters ko": "rapid",
            "speed chess gp1 2021": "blitz",
            "speed chess gp4 2021": "blitz",
            "chess super league 2021": "rapid",
            "fide grand prix 1 pool a": "rapid",
            "fide grand prix 1 playoff": "rapid",
            "fide grand prix 3 pool a": "rapid",
            "fide grand prix 3 playoff": "rapid",
            "chess.com rcc wk11 swiss": "rapid",
            "chess.com rcc wk11 ko": "blitz",
            "chess.com rcc wk12 swiss": "rapid",
            "chess.com rcc wk12 ko": "blitz",
            "chess.com rcc wk13 swiss": "rapid",
            "chess.com rcc wk13 ko": "blitz",
            "chess.com rcc wk14 swiss": "rapid",
            "chess.com rcc wk14 ko": "blitz",
            "chess.com rcc wk15 swiss": "rapid",
            "chess.com rcc wk15 ko": "blitz",
            "chess.com rcc wk16 swiss": "rapid",
            "chess.com rcc wk16 ko": "blitz",
            "chess.com rcc wk17 swiss": "rapid",
            "chess.com rcc wk17 ko": "blitz",
            "chess.com rcc wk18 swiss": "rapid",
            "chess.com rcc wk18 ko": "blitz",
            "chess.com rcc wk20 swiss": "rapid",
            "chess.com rcc wk20 ko": "blitz",
            "chess.com rcc wk22 swiss": "rapid",
            "chess.com rcc wk22 ko": "blitz",
            "chess.com rcc wk23 swiss": "rapid",
            "chess.com rcc wk24 swiss": "rapid",
            "chess.com rcc wk24 ko": "blitz",
            "chess.com rcc wk25 swiss": "rapid",
            "chess.com rcc wk25 ko": "blitz",
            "chess.com rcc winners": "rapid",
            "chess.com rcc losers": "rapid",
            "cgc ko 2022": "blitz",
            "airthings play-in match": "rapid",
            "american cup champ": "classical",
            "pro league ko 2023": "rapid",
            "chesskid cup div 1 w": "classical",
            "chesskid cup div 1 l": "classical",
            "ai cup play-in match 2023": "rapid",
            "ai cup play-in 2023": "rapid",
            "ai cup div 1 l": "classical",
            "fide grand swiss 2023": "classical",
            "cct final survival loser": "rapid",
            "chessable masters play-in": "rapid",
            "chessable play-in match d1": "rapid",
            "chessable masters div 2 w": "rapid",
            "chessable masters div 2 l": "rapid",
            "fsima feb": "rapid",
            "59th ny masters": "classical",
            "marshall v mechanics match": "classical",
            "82nd ny masters": "classical",
            "90th ny masters": "classical",
            "96th ny masters": "classical",
            "100th ny masters": "classical",
            "fsim june": "rapid",
            "119th ny masters": "classical",
            "120th ny masters": "classical",
            "122nd ny masters": "classical",
            "fsima february": "rapid",
            "fsim april": "rapid",
            "fsim may": "rapid",
            "fsim december": "rapid",
            "4th young stars of the world": "classical",
            "mitropa cup men": "classical",
            "66th ch-ita": "classical",
            "66th ch-ita playoff": "classical",
            "capo d'orso": "classical",
            "iii euicc": "classical",
            "masters": "classical",
            "67th ch-ita": "classical",
            "corus c": "classical",
            "ii ruy lopez masters": "classical",
            "ix eicc": "classical",
            "mitropa cup m": "classical",
            "3rd nh": "classical",
            "68th ch-ita": "classical",
            "magistral": "classical",
            "10th eicc": "classical",
            "16th tch-rus premier": "classical",
            "41st tch-ita": "classical",
            "mitropa cup": "classical",
            "iii ruy lopez": "classical",
            "17th tch-eur": "classical",
            "52nd it": "classical",
            "tch-sui natligaa": "classical",
            "11th eicc men": "classical",
            "17th tch-rus premier": "classical",
            "tch-sui 2010": "classical",
            "42nd tch-ita masters": "classical",
            "4th ruy lopez masters": "classical",
            "26th european club cup": "classical",
            "70th ch-ita": "classical",
            "53rd masters": "classical",
            "tch-sui 2011": "classical",
            "12th ch-eur": "classical",
            "tch-rus premier": "classical",
            "43rd tch-ita 2011": "classical",
            "tch-fra top 12 2011": "classical",
            "12th karpov int": "classical",
            "71st ch-ita 2011": "classical",
            "tch-sui national a 2012": "classical",
            "13th eicc": "classical",
            "19th tch-rus 2012": "classical",
            "30th greek cup 2012": "classical",
            "40th tch-gre 2012": "classical",
            "zuerich chess challenge": "classical",
            "20th tch-rus 2013": "classical",
            "5th classic gpd 2013": "classical",
            "millionaire tb a 2015": "blitz",
            "gashimov mem tb 2016": "blitz",
            "sinquefield gct tb 2018": "blitz",
            "chessbrah inv may 2020": "rapid",
            "superbet classic 2021": "classical",
            "ch-usa 2021": "classical",
            "ch-usa tb 2021": "blitz",
            "fide grand swiss 2021": "classical",
            "7th gashimov mem tb": "blitz",
            "superbet classic 2022": "classical",
            "chess.com rcc wk21 swiss": "rapid",
            "ch-usa 2022": "classical",
            "american cup elim": "classical",
            "superbet classic 2023": "classical",
            "julius baer play-in 2023": "rapid",
            "julius baer play-in match": "rapid",
            "ai cup div 2 w": "rapid",
            "ai cup div 2 l": "rapid",
            "ch-usa 2023": "classical",
            "cct final survival winner": "rapid",
            "chess.com classic play-in": "rapid",
            "chess.com play-in match div 1": "rapid",
            "chess.com classic div 2 w": "rapid",
            "chess.com classic div 2 l": "rapid",
            "superbet rom classic 2024": "classical"
        }

        # Check if any keyword in the mapping is in the event header
        for keyword, event_type in event_mapping.items():
            if keyword in event_header:
                return event_type
        
        # Check if the game is of "daily" type based on time control header starting with "1/"
        if time_control_header.startswith("1/"):
            return "daily"

        # Fallback to time control header if no keyword matches
        if time_control_header:
            try:
                base_time_str = time_control_header.split('+')[0]
                base_time = int(base_time_str)  # Convert base time to integer
                if 1 <= base_time < 180:
                    return "bullet"
                elif 180 <= base_time < 600:
                    return "blitz"
                elif 600 <= base_time < 1800:
                    return "rapid"
                elif base_time >= 1800:
                    return "classical"
            except ValueError:
                pass

        # Return an empty string if no conditions are met
        return ""

    def add_headers_to_pgn(self, input_pgn, output_pgn):
        with open(input_pgn, 'r') as pgn_file:
            games = []
            while True:
                game = chess.pgn.read_game(pgn_file)
                if game is None:
                    break
                games.append(game)

        with open(output_pgn, 'w') as output_file:
            for index, game in enumerate(games):
                game.headers["ID"] = str(index + 1)
               
                site_header = game.headers.get("Site", "")
                event_header = game.headers.get("Event", "")
                link_header = game.headers.get("Link", "")
                game.headers["Place"] = self.get_place(site_header, event_header, link_header)
                
                time_control_header = game.headers.get("TimeControl", "")
                time_control = self.get_time_control(event_header, time_control_header)
                if time_control:
                    game.headers["Time_Control"] = time_control
               
                output_file.write(str(game))
                output_file.write("\n\n")

    def analyze_pgn_for_missing_timecontrol(self, input_file, output_csv):
        missing_timecontrol = []
        current_game: Dict[str, Union[None, int, str]] = {'ID': None, 'Event': None}
        game_count = 0
        with open(input_file, 'r', encoding='utf-8') as pgn_file:
            for line in pgn_file:
                if line.startswith('[Event '):
                    game_count += 1
                    current_game['ID'] = game_count 
                    current_game['Event'] = re.search(r'"(.+)"', line).group(1)
                elif line.startswith('[Time_Control '):
                    current_game = {'ID': None, 'Event': None}
                elif line.strip() == '' and current_game['ID'] is not None:
                    missing_timecontrol.append(current_game)
                    current_game = {'ID': None, 'Event': None}
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['ID', 'Event']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for game in missing_timecontrol:
                writer.writerow(game)
        
        return len(missing_timecontrol)

    def extract_unique_events(self, file_paths):
        all_events = set()
        
        for file_path in file_paths:
            df = pd.read_csv(file_path)
            events = df['Event'].dropna().unique()
            all_events.update(events)
        
        return sorted(all_events)

    def process_all_pgn_files(self):
        missing_info_files = []
        for input_directory, (output_directory, output_filename) in self.pgn_directories.items():
            # Concatenate the PGN files
            concatenated_pgn_path = os.path.join(output_directory, output_filename)
            self.concatenate_pgn_files(input_directory, output_directory, output_filename)
            
            # Split the filename and extension
            base_name, ext = os.path.splitext(output_filename)

            # Remove the leading number and underscore (if present)
            base_name_without_prefix = base_name.split('_', 1)[-1]
            
            # Construct the sorted output filename
            sorted_output_filename = f"2_{base_name_without_prefix.replace('combined_games', 'combined_games_sorted')}{ext}"
            sorted_output_path = os.path.join(output_directory, sorted_output_filename)
            
            # Sort the concatenated PGN file
            self.sort_pgn_file(concatenated_pgn_path, sorted_output_path)

            # Add additional headers to the sorted PGN file
            headers_added_output_filename = f"3_{base_name_without_prefix.replace('combined_games', 'combined_games_sorted_with_added_headers')}{ext}"
            headers_added_output_path = os.path.join(output_directory, headers_added_output_filename)
            self.add_headers_to_pgn(sorted_output_path, headers_added_output_path)

            # Analyze for missing time control
            missing_info_csv = os.path.join(output_directory, f"4_{base_name_without_prefix.split('_')[0]}_missing_info.csv")
            missing_count = self.analyze_pgn_for_missing_timecontrol(headers_added_output_path, missing_info_csv)
            print(f"{base_name_without_prefix.split('_')[0].capitalize()}: {missing_count} games missing Time_Control")
            missing_info_files.append(missing_info_csv)

        # Extract all unique events from the missing info CSVs
        unique_events = self.extract_unique_events(missing_info_files)

        # Save the unique events to a text file
        output_missing_data_txt = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2\data\logs\missing_info.txt'
        with open(output_missing_data_txt, 'w') as file:
            for event in unique_events:
                file.write(f'"{event}":\n')
          
