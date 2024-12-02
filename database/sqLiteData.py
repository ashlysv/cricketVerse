import json
import sqlite3
import os


class CricketDataLoader:
    def __init__(self, db_path, json_folder):
        self.db_path = db_path
        self.json_folder = json_folder
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        """Create necessary tables in the SQLite database."""
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS Matches (
                match_id TEXT PRIMARY KEY,
                date TEXT,
                venue TEXT,
                city TEXT,
                event_name TEXT,
                match_number INTEGER,
                gender TEXT,
                match_type TEXT,
                season TEXT,
                team_type TEXT,
                toss_winner TEXT,
                toss_decision TEXT,
                winner TEXT
            );

            CREATE TABLE IF NOT EXISTS Teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT,
                team_name TEXT,
                FOREIGN KEY (match_id) REFERENCES Matches(match_id)
            );

            CREATE TABLE IF NOT EXISTS Players (
                player_id TEXT PRIMARY KEY,
                team_id INTEGER,
                player_name TEXT,
                FOREIGN KEY (team_id) REFERENCES Teams(team_id)
            );

            CREATE TABLE IF NOT EXISTS Innings (
                innings_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT,
                team_batting TEXT,
                FOREIGN KEY (match_id) REFERENCES Matches(match_id)
            );

            CREATE TABLE IF NOT EXISTS Deliveries (
                delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                innings_id INTEGER,
                over INTEGER,
                ball INTEGER,
                batter TEXT,
                bowler TEXT,
                non_striker TEXT,
                runs_batter INTEGER,
                runs_extras INTEGER,
                runs_total INTEGER,
                extras_type TEXT,
                FOREIGN KEY (innings_id) REFERENCES Innings(innings_id)
            );
        ''')

    def load_data(self):
        """Load JSON files from the specified folder into the database."""
        for filename in os.listdir(self.json_folder):
            if filename.endswith(".json"):
                file_path = os.path.join(self.json_folder, filename)
                with open(file_path) as file:
                    data = json.load(file)
                    try:
                        self.insert_match_data(data)
                    except Exception as e:
                        print(f'File name : {file_path}')
                        print(e)

    def insert_match_data(self, data):
        """Insert match, team, player, innings, and delivery data into the database."""
        match_info = data['info']
        match_number = match_info['event'].get('match_number', 0)
        match_id = f"{match_info['event']['name']}_{match_number}"

        outcome = match_info['outcome']
        winner = outcome.get('winner')
        if 'result' in outcome and outcome['result'] == 'tie':
            winner = 'Tie'

        self.cursor.execute('''
            INSERT OR IGNORE INTO Matches (
                match_id, date, venue, city, event_name, match_number, gender, 
                match_type, season, team_type, toss_winner, toss_decision, winner
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            match_id, match_info['dates'][0], match_info['venue'], match_info.get('city', 'unavailable'),
            match_info['event']['name'], match_number, match_info['gender'],
            match_info['match_type'], match_info['season'], match_info['team_type'],
            match_info['toss']['winner'], match_info['toss']['decision'], winner
        ))

        self.insert_teams_and_players(match_id, match_info)
        self.insert_innings_and_deliveries(match_id, data['innings'])

    def insert_teams_and_players(self, match_id, match_info):
        """Insert team and player data for a match."""
        for team_name, players in match_info['players'].items():
            self.cursor.execute('INSERT INTO Teams (match_id, team_name) VALUES (?, ?)', (match_id, team_name))
            team_id = self.cursor.lastrowid

            for player in players:
                player_id = match_info['registry']['people'][player]
                self.cursor.execute('''
                    INSERT OR IGNORE INTO Players (player_id, team_id, player_name)
                    VALUES (?, ?, ?)
                ''', (player_id, team_id, player))

    def insert_innings_and_deliveries(self, match_id, innings_data):
        """Insert innings and delivery data for a match."""
        for inning in innings_data:
            team_batting = inning['team']
            self.cursor.execute('INSERT INTO Innings (match_id, team_batting) VALUES (?, ?)', (match_id, team_batting))
            innings_id = self.cursor.lastrowid

            for over_info in inning['overs']:
                over_number = over_info['over']

                for ball_number, delivery in enumerate(over_info['deliveries'], start=1):
                    extras_type = next(iter(delivery.get('extras', {})), None)
                    self.cursor.execute('''
                        INSERT INTO Deliveries (
                            innings_id, over, ball, batter, bowler, non_striker, 
                            runs_batter, runs_extras, runs_total, extras_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        innings_id, over_number, ball_number, delivery['batter'], delivery['bowler'],
                        delivery.get('non_striker'), delivery['runs']['batter'],
                        delivery['runs'].get('extras', 0), delivery['runs']['total'], extras_type
                    ))

    def close(self):
        """Commit and close the database connection."""
        self.conn.commit()
        self.conn.close()
        print("Data loaded successfully into SQLite database.")


# Usage example
json_folder_path = '/Users/ashlysv/Downloads/all_json'
db_path = 'cricket_data.db'
loader = CricketDataLoader(db_path, json_folder_path)
loader.load_data()
loader.close()
