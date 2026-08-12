import os
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google import genai
from google.genai import types
import snowflake.connector

load_dotenv()



EMBEDDING_MODEL = "gemini-embedding-001"
CHAT_MODEL = "gemini-3.1-flash-lite"
NEW_REVIEWS = 50
TOP_K = 5
CACHE_FILE = "review_embeddings.parquet"


client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def read_reviews_from_snowflake():
    
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    query = f"""
        SELECT REVIEW_ID, CITY, RATING,COMMENT FROM
        ZOMATO.STAGING.STG_REVIEWS SAMPLE ({NEW_REVIEWS} ROWS)
    """

    df = conn.cursor().execute(query).fetch_pandas_all()
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    return df


def embed(texts):
    response = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=texts
    )
    embeddings = [item.values for item in response.embeddings]
    return embeddings

@st.cache_data()
def load_reviews():
    if(os.path.exists(CACHE_FILE)):
        return pd.read_parquet(CACHE_FILE)

    df = read_reviews_from_snowflake()
    df["embedding"] = embed(df["comment"].tolist())
    df.to_parquet(CACHE_FILE)
    return df

def find_similar_reviews(question, review_df):
    question_embedding = embed([question])[0]
    scores = []
    for review_embedding in review_df["embedding"]:
        # Cosine Similarity
        score = np.dot(question_embedding, review_embedding) / (np.linalg.norm(question_embedding) * np.linalg.norm(review_embedding))
        scores.append(score)
    review_df["similarity_score"] = scores
    top_reviews = review_df.nlargest(TOP_K, "similarity_score")
    return top_reviews

def ask_llm(question, top_reviews):
    context = ""

    for _, row in top_reviews.iterrows():
        context += f" ({row['city']}, {row['rating']} stars) {row['comment']}\n"

    system_prompt = (
        "Answer only the customer reviews provided."
        "Be concise. If the reviews don't covert it, say so"
    )

    user_prompt = f"Questions : {question}\n\n Reviews:\n {context}"

    response = client.models.generate_content(
    model=CHAT_MODEL,
    contents=user_prompt,
    config=types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=0.2,
    ),
    )
    return response.text



st.title("Chat with your data using RAG (Retrieval-Augmented Generation)")

st.caption(
    "This app allows you to chat with your data using RAG (Retrieval-Augmented Generation). The app uses the Gemini API to generate responses based on the data in the Snowflake database.")


review_df = load_reviews()

question = st.text_input("Ask a question about your reviews:", placeholder="What are the most common complaints about delivery ?")

if question:
    top_reviews = find_similar_reviews(question, review_df)
    answer = ask_llm(question, top_reviews)

    st.markdown(f"**Answer:**")
    st.write(answer)

    with st.expander("Reviews used to build the answer"):
        st.dataframe(top_reviews[['city','rating','comment','similarity_score']], hide_index = True)

