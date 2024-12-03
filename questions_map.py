import json
import os
import sqlite3
from http.client import HTTPException
import spacy
from openai import OpenAI

nlp = spacy.load("en_core_web_sm")
DATABASE_PATH = "./database/cricket_data.db"

# Database schema information (headers and sample rows for context)
SCHEMA_INFO = """
The database has the following tables:
1. Deliveries: delivery_id, innings_id, over, ball, batter, bowler, non_striker, runs_batter, runs_extras, runs_total, extras_type.
   Example row: 1, 1, 0, 1, AN Kervezee, NN Odhiambo, ES Szwarczynski, 1, 0, 1.

2. Innings: innings_id, match_id, team_batting.
   Example row: 1, ICC Intercontinental Shield_0, Netherlands.

3. Matches: match_id, date, venue, city, event_name, match_number, gender, match_type, season, team_type, toss_winner, toss_decision, winner.
   Example row: ICC Intercontinental Shield_0, 2010-02-20, Gymkhana Club Ground, Nairobi, ICC Intercontinental Shield, 0, male, MDM, 2009/10, international, Netherlands, bat, Kenya.

4. Players: player_id, team_id, player_name.
   Example row: dc36a6a5, 1, AN Kervezee.

5. Teams: team_id, match_id, team_name.
   Example row: 1, ICC Intercontinental Shield_0, Netherlands.
"""
OPEN_API_KEY = os.environ['OPEN_API_KEY']

client = OpenAI(
    api_key=OPEN_API_KEY
)


# Function to connect to the database
def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query_gpt(question):
    try:
        # Use ChatGPT to determine the relevant table
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system",
                 "content": f"You are a helpful assistant that maps user questions to database tables and also provides human-readable explanations. Here's the schema information:\n{SCHEMA_INFO}"},
                {"role": "user",
                 "content": f"Which table should be queried for this question: '{question}'? Please return the result as a JSON object with two keys: 'sql' for the query string and 'explanation' for a human-readable one liner of the result, e.g., {{\"sql\": \"SELECT * FROM table_name WHERE condition;\", \"explanation\": \"This query retrieves the top player with the highest runs scored.\"}}"}
            ],
            temperature=0
        )

        # Extract GPT's response
        gpt_response = response.choices[0].message.content

        # Parse the response as JSON
        response_json = json.loads(gpt_response)

        # Return SQL query and explanation
        return response_json.get('sql'), response_json.get('explanation')
    except Exception as e:
        print(HTTPException(f"Error processing the request: {str(e)}"))
        return None, None


def answers(question):
    try:
        sql_query, explanation = query_gpt(question)
        # gpt_response = 'SELECT batter AS player_name, SUM(runs_total) AS total_runs FROM Deliveries GROUP BY batter ORDER BY total_runs DESC LIMIT 1;'
        if sql_query:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql_query)
                result = cursor.fetchone()
                if result:
                    # Convert row to dictionary
                    result_dict = dict(result)
                    # Construct a human-readable output
                    readable_result = ", ".join(f"{key}: {value}" for key, value in result_dict.items())
                    return f"{explanation}\nResult: {readable_result}."
                # return "No result found or unable to process the query."

            # Use NLP to parse the question
        doc = nlp(question.lower())
        keywords = [token.lemma_ for token in doc if token.pos_ in ("NOUN", "PROPN")]
        entities = {ent.label_: ent.text for ent in doc.ents}
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


ans = answers('How many teams have played in the last 5 years')
print(ans)
