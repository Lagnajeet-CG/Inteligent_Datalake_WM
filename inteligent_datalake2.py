import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
import numpy as np
import json

# --- Custom CSS for modern look ---
st.markdown("""
    <style>
    .main {
        background-color: #f6f8fa;
    }
    .stChatInput input {
        border-radius: 8px;
        border: 1px solid #d1d5db;
        padding: 10px;
        font-size: 16px;
    }
    .card {
        background: #fff;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        padding: 18px 22px;
        margin-bottom: 18px;
    }
    .stButton>button {
        border-radius: 8px;
        background: #2563eb;
        color: white;
        font-weight: 600;
        padding: 8px 18px;
    }
    .stTextArea textarea {
        border-radius: 8px;
        font-size: 15px;
    }
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
    </style>
""", unsafe_allow_html=True)

# --- Main Title ---
st.markdown("<h1 style='color:#2563eb; margin-bottom:0;'>Intelligent DataLake</h1>", unsafe_allow_html=True)
st.caption("Ask questions about your data. Get instant SQL and natural language answers.")

# --- API & BigQuery Setup ---
key = "AIzaSyCw2EGbX55HV5PcqVVjS2LV0nXi8awGEEQ"
genai.configure(api_key=key)
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
}

import json
service_account_info = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = 'data-driven-cx'
client = bigquery.Client(credentials=credentials, project=project_id)


# --- Session State ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# --- User Input ---
user_input = st.chat_input("Ask a question about the data...")

# --- Schema Fetch ---
dataset_id = "Inteligent_datalake"
def fetch_table_schemas(project_id, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)
    all_schemas_info = ""
    for table in tables:
        table_ref = dataset_ref.table(table.table_id)
        try:
            table = client.get_table(table_ref)
            schema_str = f"Schema for table {table.table_id}:\n"
            for field in table.schema:
                schema_str += f"  {field.name} ({field.field_type})\n"
            all_schemas_info += schema_str + "\n"
        except Exception as e:
            st.error(f"Table {table.table_id} not found.")
    return all_schemas_info

if "schema" not in st.session_state:
    st.session_state.schema = []

if st.session_state.schema == []:
    with st.spinner("Loading schema information..."):
        schema_for_tables = fetch_table_schemas(project_id, dataset_id)
        st.session_state.schema.append(schema_for_tables)

# --- Query Execution ---
def execute_query(query):
    try:
        query_job = client.query(query)
        results = query_job.result().to_dataframe()
        return results
    except Exception as e:
        st.error(f"Query execution error: {e}")
        return None

def qgen(prompt):
    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        generation_config=generation_config,
    )
    response = model.generate_content(prompt)
    return response.text

# --- Main Chat Logic ---
if user_input:
    my_prompt = f"""act as a sql query writer for BigQuery database. We have the following schema:
    project_id = \"data-driven-cx\"
    dataset_id = \"Inteligent_datalake\"
    {st.session_state.schema[0]}
    Write a SQL query for user input
    user input-{user_input}.
    Write only the executable query without any comments or additional text.
    """
    with st.spinner("Generating query..."):
        final_query = qgen(my_prompt)
        cleaned_query = final_query.replace("```sql", "").replace("```", "").strip()
    try:
        with st.spinner("Executing query..."):
            data = execute_query(cleaned_query)

        if data is not None and not data.empty:
            sample = data.head(5).to_markdown(index=False)
            summary_prompt = f"""
                Act as a data analyst.
                Provide a concise natural language summary of the following data.

                User question: {user_input}
                Top 5 rows:
                {sample}
            """
            natural_language_answer = qgen(summary_prompt)
        else:
            natural_language_answer = "No results found or query failed."

        st.session_state.messages.append({
            "role": "user",
            "content": user_input
        })
        st.session_state.messages.append({
            "role": "assistant",
            "content": natural_language_answer,
            "results": data,
            "sql": cleaned_query
        })

    except Exception as e:
        st.error(f"Query execution error: {e}")

# --- Chat History Display ---
st.markdown("## 💬 Conversation")
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        st.markdown(f"<div class='card'>{message['content']}</div>", unsafe_allow_html=True)
        if "results" in message:
            if message["results"] is not None and not message["results"].empty:
                st.dataframe(message["results"])

# --- Last SQL ---
if st.session_state.messages:
    with st.expander('📝 Last generated SQL:'):
        last_msg = st.session_state.messages[-1]
        st.code(last_msg.get("sql", ""), language='sql')
