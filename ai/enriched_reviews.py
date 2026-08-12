import os
import json
import snowflake.connector
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

MODEL = "gemini-3.1-flash-lite"
SAMPLE_N = 15
TOPICS = []

client = genai.Client(api_key= os.getenv("GEMINI_API_KEY"))

SYSTEM_PROMPT = """
    You classify customer reviews for a delivery app.
    For the review you are given, Return:
    - sentiment_label : positive, negative, or neutral
    - sentiment_score : a number between -1.0 and 1.0
    - topic: one of {TOPICS}
    - key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null

    Reply as JSON in this exact format:
    {{
        sentiment_label: <sentiment_label>,
        sentiment_score: <sentiment_score>,
        topic: <topic>,
        key_issue: <key_issue>
    }}
"""

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment_label": {"type":"STRING", "enum": ["positive", "negative", "neutral"]},
        "sentiment_score": {"type":"NUMBER"},
        "topic": {"type":"STRING", "enum": TOPICS},
        "key_issue": {"type":"STRING", "nullable": True}
    },
    "required": ["sentiment_label", "sentiment_score", "topic", "key_issue"]
}

def get_connection():
    return snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_USER"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        account = os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema = os.getenv("SNOWFLAKE_SCHEMA")
    )

def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.ENRICHED_REVIEWS (
            review_id STRING,
            sentiment_label STRING,
            sentiment_score FLOAT,
            topic STRING,
            key_issue STRING,
            MODEL STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

def get_reviews_to_enrich(cursor):
    # cursor.execute("USE WAREHOUSE ZOMATO_WH")
    cursor.execute(f"""
        SELECT REVIEW_ID, COMMENT FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.ENRICHED_REVIEWS)
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()


def classify_review(comment):
    response = client.models.generate_content(
        model=MODEL,
        contents = comment,
        config= types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=0,
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,
        )
    )
    return json.loads(response.text)


def save_results(cursor,results):
    print(f"saving {len(results)} reviews to Snowflake")
    cursor.executemany("""
        INSERT INTO ZOMATO.AI.ENRICHED_REVIEWS (review_id, sentiment_label, sentiment_score, topic, key_issue, MODEL)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, results)


def main():
    conn = get_connection()
    cursor = conn.cursor()
    create_output_table(cursor)
    reviews = get_reviews_to_enrich(cursor)
    if(len(reviews) == 0):
        print('No new reviews to enrich')
        return
    print(f"Enriching {len(reviews)} reviews")
    results = []
    for review_id, comment in reviews:
        print(f"classifying review {review_id}: {comment}")
        try:
            labels = classify_review(comment)
            print(f"label for review {review_id}: {labels}")
            results.append((
                review_id,
                labels["sentiment_label"],
                labels["sentiment_score"],
                labels["topic"],
                labels["key_issue"],
                MODEL
            ))
        except Exception as e:
            print(f"Error classifying review {review_id}: {e}")
    
    save_results(cursor, results)
    print("Done enriching reviews")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()