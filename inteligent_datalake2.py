import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
import json

# --- Custom CSS ---
st.markdown("""
    <style>
    .main { background-color: #f6f8fa; }
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


# --- API & BigQuery Setup ---
genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
}

service_account_info = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = 'data-driven-cx'
client = bigquery.Client(credentials=credentials, project=project_id)

# --- Dataset Selection ---
st.sidebar.header("🔄 Choose Dataset")
dataset_id = st.sidebar.selectbox("Select Dataset", ["Inteligent_datalake", "BANKING_GSIS"])

# --- Reset schema if dataset changes ---
if "last_selected_dataset" not in st.session_state:
    st.session_state.last_selected_dataset = dataset_id
if dataset_id != st.session_state.last_selected_dataset:
    st.session_state.schema = []
    st.session_state.last_selected_dataset = dataset_id

# # --- Upload Excel File to BigQuery UI ---
# st.subheader("📤 Upload Excel File to BigQuery")

# uploaded_file = st.file_uploader("Choose an Excel file (.xlsx)", type=["xlsx"])
# if uploaded_file:
#     df = pd.read_excel(uploaded_file)
#     st.write("Preview of Uploaded Data:")
#     st.dataframe(df)

#     table_name = st.text_input("Enter BigQuery Table Name")
#     if table_name and st.button("🚀 Upload to BigQuery"):
#         try:
#             table_id = f"{project_id}.{dataset_id}.{table_name}"
#             job = client.load_table_from_dataframe(df, table_id)
#             job.result()
#             st.success(f"✅ Successfully uploaded to `{table_id}`")
#         except Exception as e:
#             st.error(f"❌ Upload failed: {e}")

# --- Schema Fetch Function ---
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
    with st.spinner("🔍 Loading schema..."):
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

# --- Chat Input ---
user_input = st.chat_input("Ask a question about your data...")

# --- Main Chat Logic ---
if user_input:
    my_prompt = f"""Act as a SQL query writer for BigQuery.
We have the following schema:
project_id = \"{project_id}\"
dataset_id = \"{dataset_id}\"
{st.session_state.schema[0]}
User question: {user_input}
Write only the executable query."""
    with st.spinner("🤖 Generating SQL..."):
        final_query = qgen(my_prompt)
        cleaned_query = final_query.replace("```sql", "").replace("```", "").strip()

    try:
        with st.spinner("📊 Executing query..."):
            data = execute_query(cleaned_query)

        if data is not None and not data.empty:
            top_rows = data.head(5).to_markdown(index=False)
            summary_prompt = f"""Act as a data analyst.
Summarize the following query result in plain language.

User question: {user_input}
Top 5 rows:
{top_rows}"""
            natural_answer = qgen(summary_prompt)
        else:
            natural_answer = "No results found."

        st.session_state.setdefault("messages", []).extend([
            {"role": "user", "content": user_input},
            {"role": "assistant", "content": natural_answer, "results": data, "sql": cleaned_query}
        ])

        chart_keywords = ["graph", "chart", "bar", "plot", "visual"]
        if any(k in user_input.lower() for k in chart_keywords):
            st.markdown("### 📊 Chart")
            try:
                st.bar_chart(data)
            except:
                st.warning("Chart cannot be rendered for this data.")

    except Exception as e:
        st.error(f"Query error: {e}")

# --- Conversation History ---
st.markdown("## 💬 GSIS")
for msg in st.session_state.get("messages", []):
    with st.chat_message(msg["role"]):
        st.markdown(f"<div class='card'>{msg['content']}</div>", unsafe_allow_html=True)
        if msg.get("results") is not None:
            st.dataframe(msg["results"])

# --- Show SQL ---
if st.session_state.get("messages"):
    with st.expander("📝 Last SQL Query"):
        st.code(st.session_state["messages"][-1].get("sql", ""), language="sql")
