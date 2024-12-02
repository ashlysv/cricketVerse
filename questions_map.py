import sqlite3

DATABASE_PATH = "./database/cricket_data.db"


# Function to connect to the database
def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def answers(keywords, entities=None):
    print(keywords)
    """
    Handle user queries by analyzing keywords and entities.

    Args:
        keywords (list): List of keywords extracted from the query.
        entities (dict): Named entities extracted from the query.

    Returns:
        str: Response to the user's question.
    """
    try:

        # Winner-specific question: Who won a specific match
        if "winner" in keywords and "match" in keywords and "ORDINAL" in entities:
            match_number = entities["ORDINAL"]
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT winner FROM Matches WHERE match_number = ?;", (match_number,))
                result = cursor.fetchone()
            if result:
                return f"The winner of match {match_number} is {result['winner']}."
            else:
                return f"No data found for match {match_number}."

        # Score after 50 that batsmen get out most often
        elif "score after 50" in keywords and "batsmen" in keywords:
            sql = """
            SELECT runs_batter, COUNT(*) AS dismissal_count
            FROM Deliveries
            WHERE runs_batter > 50
            GROUP BY runs_batter
            ORDER BY dismissal_count DESC
            LIMIT 1;
            """
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
            if result:
                return f"The score after 50 that batsmen have gotten out most often is {result['runs_batter']} with {result['dismissal_count']} dismissals."

        # Which player has scored the most runs?
        elif "player" in keywords and "runs" in keywords:
            sql = """
            SELECT batter, SUM(runs_batter) AS total_runs
            FROM Deliveries
            GROUP BY batter
            ORDER BY total_runs DESC
            LIMIT 1;
            """
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
            if result:
                return f"The player with the most runs is {result['batter']} with {result['total_runs']} runs."

        # Which bowler has the most wickets?
        elif "bowler" in keywords and "wickets" in keywords:
            sql = """
            SELECT bowler, COUNT(*) AS wickets
            FROM Deliveries
            WHERE extras_type IS NULL AND runs_batter = 0
            GROUP BY bowler
            ORDER BY wickets DESC
            LIMIT 1;
            """
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
            if result:
                return f"The bowler with the most wickets is {result['bowler']} with {result['wickets']} wickets."

        # How many matches ended in a tie?
        elif "tie" in keywords and "match" in keywords:
            sql = """
            SELECT COUNT(*) AS ties
            FROM Matches
            WHERE winner IS NULL AND toss_decision = 'tie';
            """
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
            if result:
                return f"There are {result['ties']} matches that ended in a tie."

        # Which player has hit the most sixes?
        elif "player" in keywords and "sixes" in keywords:
            sql = """
            SELECT batter, COUNT(*) AS sixes
            FROM Deliveries
            WHERE runs_batter = 6
            GROUP BY batter
            ORDER BY sixes DESC
            LIMIT 1;
            """
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
            if result:
                return f"The player with the most sixes is {result['batter']} with {result['sixes']} sixes."

            # Match-specific question: Total number of matches
        elif "match" in keywords or "matches" in keywords:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) AS count FROM Matches;")
                match_count = cursor.fetchone()["count"]
            return f"There are a total of {match_count} matches recorded."

            # Team-specific question: List of teams
        elif "team" in keywords or "teams" in keywords:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT team_name FROM Teams;")
                teams = [row["team_name"] for row in cursor.fetchall()]
            return f"Teams in the dataset include: {', '.join(teams[:11])}."

            # Player-specific question: List of players
        elif "player" in keywords or "players" in keywords:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT player_name FROM Players;")
                players = [row["player_name"] for row in cursor.fetchall()]
            return f"Players in the dataset include: {', '.join(players[:11])}."

        else:
            return ("I couldn't find an answer to that. \nCould you provide more context or rephrase your question?"
                    "\nYou may try below samples: \nWhich player has scored the most runs?"
                    "\nList of teams")

    except Exception as e:
        return f"An error occurred while processing your question: {str(e)}"
