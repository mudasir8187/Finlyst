import sys, os
sys.dont_write_bytecode = True
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent
import time
import psycopg2
from dotenv import load_dotenv
import streamlit as st
import tempfile
import traceback

# Import your uploader function directly
from services.uploader import upload_erp_data  
# Import your delete function
from services.delete import delete_erp  


load_dotenv()


ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}

# Sidebar upload section
with st.sidebar:
    st.markdown("### 📤 Upload ERP Data")

    # Inputs for metadata
    user_id = st.text_input("User ID", key="sidebar_user_id")
    erp_name = st.text_input("ERP Name", key="sidebar_erp_name")

    # File uploader
    uploaded_files = st.file_uploader(
        "Upload CSV/XLS/XLSX files",
        type=["csv", "xls", "xlsx"],
        accept_multiple_files=True,
        key="sidebar_file_uploader"
    )

    if st.button("Upload Files", key="sidebar_upload_btn"):
        if not user_id or not erp_name:
            st.error("⚠️ Please provide both User ID and ERP Name.")
        elif not uploaded_files:
            st.error("⚠️ Please upload at least one file.")
        else:
            results = []
            for file in uploaded_files:
                ext = os.path.splitext(file.name)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    results.append({
                        "file_name": file.name,
                        "status": "error",
                        "message": f"Invalid file type '{ext}'. Only CSV, XLS, XLSX are allowed."
                    })
                    continue

                try:
                    # Save file temporarily
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        tmp.write(file.read())
                        tmp_path = tmp.name

                    # Call the ERP uploader logic directly
                    upload_erp_data(user_id, erp_name, tmp_path)

                    results.append({
                        "file_name": file.name,
                        "status": "success",
                        "message": "ERP data uploaded successfully"
                    })

                except Exception as exc:
                    error_trace = traceback.format_exc()
                    results.append({
                        "file_name": file.name,
                        "status": "error",
                        "message": str(exc),
                        "traceback": error_trace
                    })

                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

            # Show results inside sidebar
            for r in results:
                if r["status"] == "success":
                    st.success(f"✅ {r['file_name']}: {r['message']}")
                    st.cache_data.clear()
                else:
                    st.error(f"❌ {r['file_name']}: {r['message']}")
                    with st.expander(f"Show Traceback ({r['file_name']})"):
                        st.text(r.get("traceback", "No traceback"))


with st.sidebar:
    st.markdown("### 🗑️ Delete ERP Data")

    # Inputs for deletion
    del_user_id = st.text_input("User ID (Delete)", key="sidebar_del_user_id")
    del_erp_name = st.text_input("ERP Name (Delete)", key="sidebar_del_erp_name")

    if st.button("Delete ERP Data", key="sidebar_delete_btn"):
        if not del_user_id or not del_erp_name:
            st.error("⚠️ Please provide both User ID and ERP Name for deletion.")
        else:
            try:
                start_time = time.time()

                # Call the ERP delete logic directly
                delete_erp(del_user_id, del_erp_name)

                execution_time = round(time.time() - start_time, 2)
                st.success(f"✅ ERP data '{del_erp_name}' deleted successfully for user '{del_user_id}' in {execution_time} sec.")
                st.cache_data.clear()

            except Exception as exc:
                error_trace = traceback.format_exc()
                st.error(f"❌ Failed to delete ERP data: {str(exc)}")
                with st.expander("Show Traceback"):
                    st.text(error_trace)

@st.cache_data(ttl=0)  # Cache for 5 minutes
def get_postgres_databases(host, port, user, password):
    """Fetch list of all non-template databases from PostgreSQL server"""
    try:
        # Connect to PostgreSQL (default DB is 'postgres')
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Query to list databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        
        databases = [db[0] for db in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return databases

    except Exception as e:
        # Return empty list on error (error will be handled in UI)
        return []

@st.cache_data(ttl=0)  # Cache for 5 minutes
def get_postgres_tables(dbname, host, port, user, password):
    """Fetch list of tables from a specific PostgreSQL database"""
    try:
        # Connect to the selected database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Fetch tables only from public schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        return tables

    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []


# Set page configuration
st.set_page_config(
    page_title=" Finlyst Assistant",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for beautiful styling
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stButton>button {
        background-color: #4C8BF5;
        color: white;
        border-radius: 8px;
        height: 3.5rem;
        font-size: 1.1rem;
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        background-color: #3a76e0;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(76, 139, 245, 0.3);
    }
    .stTextInput>div>div>input {
        border-radius: 8px;
        padding: 12px;
        font-size: 1.1rem;
    }
    .result-box {
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-top: 20px;
    }
    .header {
        text-align: center;
        padding: 20px 0;
        margin-bottom: 20px;
    }
    .header h1 {
        color: #2c3e50;
        font-weight: 700;
    }
    .header p {
        color: #7f8c8d;
        font-size: 1.1rem;
    }
    .example-questions {
        background-color: #f1f8ff;
        border-radius: 8px;
        padding: 15px;
        margin-top: 20px;
    }
    .example-questions h4 {
        color: #4C8BF5;
        margin-bottom: 10px;
    }
    .example-btn {
        display: inline-block;
        background-color: #e3f2fd;
        color: #1976d2;
        padding: 8px 15px;
        border-radius: 20px;
        margin: 5px;
        cursor: pointer;
        transition: all 0.2s;
        font-size: 0.9rem;
    }
    .example-btn:hover {
        background-color: #bbdefb;
        transform: translateY(-2px);
    }
</style>
""", unsafe_allow_html=True)

# Header section
st.markdown("""
<div class="header">
    <h1>📊 Finlyst Assistant</h1>
    <p>Ask questions about your database in natural language</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
    
if "current_query" not in st.session_state:
    st.session_state.current_query = ""
    
if "query_results" not in st.session_state:
    st.session_state.query_results = None


# Sidebar configuration
with st.sidebar:
    st.title("Configuration")
    
    st.markdown("### 🗄️ Database Connection")
    
    db_host = st.secrets["db_host"]  #os.getenv("db_host")  #st.text_input("Host", value=os.getenv("db_host", "localhost"))
    db_port = st.secrets["db_port"] #st.text_input("Port", value=os.getenv("db_port", "5432"))
    db_name = st.secrets["db_name"]  #st.text_input("Database Name", value=os.getenv("db_name", "user_1"))
    db_user = st.secrets["db_user"]  #st.text_input("Username", value=os.getenv("db_user", "postgres"))
    db_password = st.secrets["db_password"]#st.text_input("Password", value=os.getenv("db_password", ""), type="password")

    
    # Only try to fetch databases if connection parameters are provided
    if db_host and db_port and db_user and db_password:
        with st.spinner("Fetching databases..."):
            databases = get_postgres_databases(db_host, db_port, db_user, db_password)
        
        if not databases:
            st.error("Failed to fetch databases. Check your connection details.")
            db_name = st.text_input("Database Name", value= st.secrets["db_name"]) #os.getenv("db_name", "user_1"))
        else:
            # Set default selection to current db_name if it exists in the list
            default_index = 0
            if st.secrets["db_name"] in databases:
                default_index = databases.index(st.secrets["db_name"])
            elif "postgres" in databases:
                default_index = databases.index("postgres")
                
            db_name = st.selectbox(
                "Database", 
                options=databases, 
                index=default_index,
                key="database_select",
                help="Select a database from your PostgreSQL server"
            )
            
            # Store selected database in session state
            st.session_state.selected_database = db_name
            
            # Now fetch tables for the selected database
            with st.spinner(f"Fetching tables for '{db_name}'..."):
                tables = get_postgres_tables(db_name, db_host, db_port, db_user, db_password)
            
            if not tables:
                st.warning(f"No tables found in database '{db_name}' or failed to fetch tables.")
            else:
                # Add table selection dropdown
                st.session_state.available_tables = tables
                
                selected_table = st.selectbox(
                    "Select Table",
                    options=tables,
                    index=0,
                    key="table_select",
                    help="Choose a specific table to analyze"
                )
                
                # Store selected table in session state
                st.session_state.selected_table = selected_table
                
                # Option to show table structure
                if st.checkbox("Show table structure", key="show_structure"):
                    try:
                        conn = psycopg2.connect(
                            dbname=db_name,
                            user=db_user,
                            password=db_password,
                            host=db_host,
                            port=db_port
                        )
                        cursor = conn.cursor()
                        
                        # Get columns for the selected table
                        cursor.execute("""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_schema = 'public' AND table_name = %s
                            ORDER BY ordinal_position;
                        """, (selected_table,))
                        
                        columns = cursor.fetchall()
                        cursor.close()
                        conn.close()
                        
                        if columns:
                            st.markdown("**Columns:**")
                            for col in columns:
                                st.markdown(f"- `{col[0]}` (`{col[1]}`)")
                    except Exception as e:
                        st.error(f"Error fetching table structure: {str(e)}")
    else:
        db_name = st.text_input("Database Name", value= st.secrets["db_name"]) #os.getenv("db_name", "user_1"))
    
    st.markdown("### 🤖 AI Model Settings")
    api_key = st.secrets["OPENROUTER_API_KEY"] #os.getenv("OPENROUTER_API_KEY")  # Keep using env variable for API key
    
    # Test connection button
    if st.button("Test Database Connection", use_container_width=True):
        try:
            test_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            test_db = SQLDatabase.from_uri(test_uri)
            tables = test_db.get_usable_table_names()
            
            with st.spinner("Testing connection..."):
                time.sleep(1)
                st.success(f"✅ Connected! Found {len(tables)} tables.")
                st.write("Tables:", ", ".join(tables[:5]) + ("..." if len(tables) > 5 else ""))
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")


# Main content area
st.subheader("Ask a question about your database")

# Example questions
st.markdown("""
<div class="example-questions">
    <h4>💡 Try these example questions:</h4>
    <div>
        <span class="example-btn">Top 5 products by sales</span>
        <span class="example-btn">Which country has the highest profit margin?</span>
        <span class="example-btn">Show me the monthly sales trend for 2023</span>
        <span class="example-btn">Which product category has the most returns?</span>
    </div>
</div>
""", unsafe_allow_html=True)

# User input
user_question = st.text_area(
    "Your question:",
    value=st.session_state.current_query,
    height=100,
    placeholder="Example: 'What are the top 5 products by sales in 2023?'"
)

col1, col2 = st.columns([1, 5])
with col1:
    submit_button = st.button("🔍 Ask Question", use_container_width=True)
with col2:
    if st.button("🧹 Clear History", use_container_width=True):
        st.session_state.messages = []
        st.session_state.current_query = ""
        st.session_state.query_results = None
        st.rerun()

# Process the question when button is clicked
if submit_button and user_question.strip():
    st.session_state.current_query = user_question
    st.session_state.query_results = None
    
    # Show loading indicator
    with st.spinner("Analyzing your question and generating response..."):
        try:
            # Construct the database URI
            db_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            
            # Initialize SQLDatabase connection
            db = SQLDatabase.from_uri(db_uri)
            
            # Initialize the LLM
            llm = ChatOpenAI(
                model="openai/gpt-4.1-mini",
                temperature=0,
                openai_api_key=api_key,
                openai_api_base="https://openrouter.ai/api/v1"
            )
            
            # Create toolkit and agent
            toolkit = SQLDatabaseToolkit(db=db, llm=llm)
            tools = toolkit.get_tools()
            
            # System prompt
            system_prompt = """
            You are an agent designed to interact with a PostgreSQL database.
            IMPORTANT: This database uses case-sensitive column names that MUST be quoted with double quotes.
            For example, use "Discount_Band" instead of Discount_Band.
            Given an input question, create a syntactically correct {dialect} query to run,
            then look at the results of the query and return the answer. Unless the user
            specifies a specific number of examples they wish to obtain, always limit your
            query to at most {top_k} results.

            You can order the results by a relevant column to return the most interesting
            examples in the database. Never query for all the columns from a specific table,
            only ask for the relevant columns given the question.

            You MUST double check your query before executing it. If you get an error while
            executing a query, rewrite the query and try again.

            DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the
            database.

            To start you should ALWAYS look at the tables in the database to see what you
            can query. Do NOT skip this step.

            Then you should query the schema of the most relevant tables.
            """.format(
                dialect=db.dialect,
                top_k=5,
            )
            
            # Create agent
            agent = create_react_agent(
                llm,
                tools,
                prompt=system_prompt,
            )
            
            # Stream the agent response and capture only the final AI message
            final_response = ""
            for step in agent.stream(
                {"messages": [{"role": "user", "content": user_question}]},
                stream_mode="values",
            ):
                # Get the last message in the step
                if "messages" in step and len(step["messages"]) > 0:
                    latest_message = step["messages"][-1]
                    
                    # Check if it's an AI message and store it
                    if hasattr(latest_message, 'type') and latest_message.type == "ai":
                        final_response = latest_message.content
            
            # Store and display only the final response
            st.session_state.query_results = final_response
            st.session_state.messages.append({
                "question": user_question,
                "answer": final_response
            })
            
        except Exception as e:
            st.error(f"❌ Error processing your request: {str(e)}")
            st.info("Possible issues:\n- Incorrect database credentials\n- Invalid API key\n- Network connectivity issues\n- Database permissions problem")

# Display chat history - only showing the final response
if st.session_state.messages:
    st.markdown("## Previous Questions & Answers")
    
    for i, msg in enumerate(reversed(st.session_state.messages)):
        with st.expander(f"**Q:** {msg['question']}", expanded=(i == 0)):
            st.markdown(f'<div class="result-box">{msg["answer"]}</div>', unsafe_allow_html=True)

# If there's a current result being displayed
if st.session_state.query_results:
    st.markdown("## Latest Response")
    st.markdown(f'<div class="result-box">{st.session_state.query_results}</div>', unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #7f8c8d; padding: 20px;">
    <p>SQL Query Assistant • Powered by LangChain & Streamlit • Analyze your database with natural language</p>
</div>
""", unsafe_allow_html=True)

