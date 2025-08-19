from typing import Literal, Dict, Any, Optional
from langchain_core.messages import AIMessage, ToolMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import (
    QuerySQLCheckerTool,
    QuerySQLDataBaseTool,
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool
)
import os
import re
from datetime import datetime

# ======================
# SECURITY CONFIGURATION
# ======================
# NEVER commit API keys to version control!
OPENAI_API_KEY = "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" #os.getenv("OPENAI_API_KEY", "sk-your-temp-key-for-dev")  # Use env vars in production
DB_PASSWORD = "moni123" #os.getenv("DB_PASSWORD", "your-temp-password")  # Use env vars in production

# Configure PostgreSQL connection using environment variables
# (Set these in your environment or .env file before running)
db_user = "postgres" #os.getenv("DB_USER", "postgres")       # e.g., "postgres"
db_password ="moni123" #os.getenv("DB_PASSWORD", "moni123")
db_host = "localhost" # os.getenv("DB_HOST", "localhost")               # e.g., "localhost" or RDS endpoint
db_port = "5432" #os.getenv("DB_PORT", "5432")                    # Default PostgreSQL port
db_name = "user_1" #os.getenv("DB_NAME", "user_1")                 # Your database name

# Construct the database URI
#db_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
# ======================
# POSTGRESQL CONNECTION
# ======================
def get_db_connection() -> SQLDatabase:
    """Safely create PostgreSQL connection with validation"""
    try:
        db = SQLDatabase.from_uri(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
            include_tables=None,  # Auto-detect all tables
            ignore_tables=None,
            sample_rows_in_table_info=3,  # Show sample data for context
            view_support=True
        )
        
        # Verify connection and schema
        tables = db.get_usable_table_names()
        if not tables:
            raise ValueError("No tables found in database. Verify Chinook schema installation.")
            
        print(f"✅ Connected to PostgreSQL. Found {len(tables)} tables: {', '.join(tables)}")
        return db
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        print("Possible fixes:")
        print("1. Verify PostgreSQL is running (check port 5432)")
        print("2. Confirm Chinook schema is installed")
        print("3. Check credentials in environment variables")
        raise

# Initialize database (fail early if connection issues)
try:
    db = get_db_connection()
    DIALECT = db.dialect  # Should be 'postgresql'
    print(f"📊 Using database dialect: {DIALECT.upper()}")
except Exception as e:
    print("💥 Critical error: Cannot proceed without database connection")
    exit(1)

# ======================
# LLM & TOOL SETUP
# ======================
from langchain.chat_models import init_chat_model

llm = init_chat_model(
    "gpt-4-turbo",
    api_key=OPENAI_API_KEY,
    temperature=0.2,  # Lower for more deterministic SQL
    max_tokens=1000
)

# Create PostgreSQL-optimized tools
tools = [
    ListSQLDatabaseTool(db=db),
    InfoSQLDatabaseTool(db=db),
    QuerySQLDataBaseTool(db=db),
    QuerySQLCheckerTool(db=db)  # For automatic query validation
]

# Map tool names to instances for easy access
tool_map = {tool.name: tool for tool in tools}

# ======================
# POSTGRESQL-SPECIFIC ENHANCEMENTS
# ======================
def postgresql_safe_query(query: str) -> str:
    """Make queries PostgreSQL-safe with case sensitivity handling"""
    # Handle PostgreSQL's case sensitivity (Chinook uses quoted identifiers)
    if not re.search(r'FROM\s+"', query, re.IGNORECASE):
        query = re.sub(
            r'(FROM|JOIN)\s+(\w+)',
            r'\1 "\2"',
            query,
            flags=re.IGNORECASE
        )
    
    # Prevent dangerous operations
    prohibited_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    for keyword in prohibited_keywords:
        if re.search(rf'\b{keyword}\b', query, re.IGNORECASE):
            raise ValueError(f"Security violation: {keyword} operations are prohibited")
    
    return query

# ======================
# AGENT GRAPH NODES
# ======================
def list_tables(state: MessagesState) -> Dict[str, Any]:
    """Safely list tables with error handling"""
    try:
        tool = tool_map["sql_db_list_tables"]
        tool_call = {
            "name": tool.name,
            "args": {},
            "id": f"table_list_{datetime.now().strftime('%H%M%S')}",
            "type": "tool_call"
        }
        
        # Execute tool
        tool_response = tool.invoke(tool_call["args"])
        return {
            "messages": [
                AIMessage(content="", tool_calls=[tool_call]),
                ToolMessage(content=tool_response, tool_call_id=tool_call["id"])
            ]
        }
    except Exception as e:
        return {
            "messages": [
                AIMessage(content=f"❌ Table listing failed: {str(e)}")
            ]
        }

def get_schema(state: MessagesState) -> Dict[str, Any]:
    """Get schema with table focus optimization"""
    try:
        # Get last user question for context
        last_user_msg = next(
            (msg.content for msg in reversed(state["messages"]) if msg.type == "human"),
            "No specific question"
        )
        
        # Focus schema retrieval on relevant tables
        relevant_tables = []
        if "artist" in last_user_msg.lower():
            relevant_tables.append("Artist")
        if "track" in last_user_msg.lower() or "song" in last_user_msg.lower():
            relevant_tables.append("Track")
        
        tool = tool_map["sql_db_schema"]
        tool_call = {
            "name": tool.name,
            "args": {"table_names": relevant_tables} if relevant_tables else {},
            "id": f"schema_{datetime.now().strftime('%H%M%S')}",
            "type": "tool_call"
        }
        
        tool_response = tool.invoke(tool_call["args"])
        return {
            "messages": [
                AIMessage(content="", tool_calls=[tool_call]),
                ToolMessage(content=tool_response, tool_call_id=tool_call["id"])
            ]
        }
    except Exception as e:
        return {
            "messages": [
                AIMessage(content=f"❌ Schema retrieval failed: {str(e)}")
            ]
        }

def generate_query(state: MessagesState) -> Dict[str, Any]:
    """Generate PostgreSQL-optimized queries"""
    system_prompt = f"""
You are a PostgreSQL expert assistant. Key rules:
1. ALWAYS use double quotes for table/column names (e.g., "Track"."Name")
2. LIMIT all SELECT queries to 50 results unless user specifies otherwise
3. For JOINs, always use explicit JOIN syntax with proper table aliases
4. When calculating averages, use CAST(column AS FLOAT) to avoid integer division
5. For date handling, use PostgreSQL functions (e.g., EXTRACT(YEAR FROM "InvoiceDate"))

Current database dialect: {DIALECT}
Relevant tables: {', '.join(db.get_usable_table_names())}

Generate ONLY the SQL query with no additional text. Example:
SELECT "Name" FROM "Artist" LIMIT 5;
"""
    
    # Add conversation history for context
    messages = [SystemMessage(content=system_prompt)] + state["messages"]
    
    # Force tool call for query generation
    llm_with_tools = llm.bind_tools(
        [tool_map["sql_db_query"]],
        tool_choice="sql_db_query"
    )
    
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}

def check_query(state: MessagesState) -> Dict[str, Any]:
    """Validate and repair PostgreSQL queries"""
    try:
        last_message = state["messages"][-1]
        if not last_message.tool_calls:
            return {"messages": [AIMessage(content="No query to validate")]}

        # Extract query from tool call
        query = last_message.tool_calls[0]["args"]["query"]
        
        # Apply PostgreSQL safety checks
        safe_query = postgresql_safe_query(query)
        
        # Use built-in checker tool
        checker = tool_map["sql_db_query_checker"]
        check_result = checker.invoke({"query": safe_query})
        
        # Process validation result
        if "error" in check_result.lower():
            # Let LLM fix the query
            fix_prompt = f"""
PostgreSQL query validation failed:
{check_result}

Original query:
{safe_query}

Fix the query following PostgreSQL syntax rules. Return ONLY the corrected SQL.
"""
            fix_response = llm.invoke([
                SystemMessage(content="You are a PostgreSQL syntax expert"),
                AIMessage(content=fix_prompt)
            ])
            fixed_query = fix_response.content.strip()
            return {
                "messages": [
                    AIMessage(
                        content="",
                        tool_calls=[{
                            "name": "sql_db_query",
                            "args": {"query": fixed_query},
                            "id": f"fixed_{last_message.id}",
                            "type": "tool_call"
                        }]
                    )
                ]
            }
        else:
            # Query is valid - pass through
            return {
                "messages": [
                    AIMessage(
                        content="",
                        tool_calls=[last_message.tool_calls[0]]  # Original valid query
                    )
                ]
            }
            
    except Exception as e:
        return {
            "messages": [
                AIMessage(content=f"❌ Query validation failed: {str(e)}")
            ]
        }

def should_continue(state: MessagesState) -> Literal[END, "check_query", "run_query"]:
    """Robust decision logic with safety checks"""
    messages = state["messages"]
    last_message = messages[-1]
    
    # Handle tool responses
    if last_message.type == "tool":
        return END  # End after successful query execution
    
    # Handle tool calls
    if last_message.tool_calls:
        # Only proceed if it's a query execution call
        if last_message.tool_calls[0]["name"] == "sql_db_query":
            return "check_query"
        return "run_query"
    
    # Handle errors or natural language responses
    if "error" in last_message.content.lower() or "failed" in last_message.content.lower():
        return END
    
    # Default: try generating a new query
    return "generate_query"

# ======================
# BUILD ROBUST AGENT GRAPH
# ======================
builder = StateGraph(MessagesState)

# Add nodes with error isolation
builder.add_node("list_tables", list_tables)
builder.add_node("get_schema", get_schema)
builder.add_node("generate_query", generate_query)
builder.add_node("check_query", check_query)
builder.add_node("run_query", ToolNode([tool_map["sql_db_query"]], name="run_query"))

# Define edges with safety fallbacks
builder.add_edge(START, "list_tables")
builder.add_edge("list_tables", "get_schema")
builder.add_edge("get_schema", "generate_query")
builder.add_conditional_edges(
    "generate_query",
    should_continue,
    {
        "check_query": "check_query",
        "run_query": "run_query",
        END: END
    }
)
builder.add_conditional_edges(
    "check_query",
    lambda state: "run_query" if state["messages"][-1].tool_calls else END,
    {"run_query": "run_query", END: END}
)
builder.add_edge("run_query", "generate_query")  # Allow follow-up queries

agent = builder.compile()

# ======================
# EXECUTION WITH SAFETY
# ======================
def safe_execute(question: str):
    """Execute with full error handling and logging"""
    print(f"\n{'='*50}\n🔍 QUESTION: {question}\n{'='*50}")
    
    try:
        # Validate input
        if not question.strip():
            print("❌ Error: Empty question provided")
            return
            
        # Stream execution with step logging
        for step in agent.stream(
            {"messages": [{"role": "user", "content": question}]},
            stream_mode="values",
            config=RunnableConfig(recursion_limit=10)  # Prevent infinite loops
        ):
            last_msg = step["messages"][-1]
            
            # Format output based on message type
            if last_msg.type == "ai" and last_msg.tool_calls:
                print(f"\n🛠️  GENERATING QUERY:")
                print(last_msg.tool_calls[0]["args"]["query"])
                
            elif last_msg.type == "tool":
                print(f"\n✅ QUERY RESULTS:")
                # Pretty print tabular results
                if "rows" in last_msg.content:
                    rows = eval(last_msg.content)["rows"]
                    if rows:
                        headers = list(rows[0].keys())
                        print("\n" + " | ".join(f"{h:^20}" for h in headers))
                        print("-" * (23 * len(headers)))
                        for row in rows[:5]:  # Show max 5 rows
                            print(" | ".join(f"{str(v)[:18]:<20}" for v in row.values()))
                        if len(rows) > 5:
                            print(f"... and {len(rows)-5} more rows")
                    else:
                        print("No results found")
                else:
                    print(last_msg.content)
                    
            elif last_msg.type == "ai" and not last_msg.tool_calls:
                print(f"\n💡 FINAL ANSWER:")
                print(last_msg.content)
                
    except Exception as e:
        print(f"\n💥 CRITICAL EXECUTION ERROR: {str(e)}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")

# ======================
# TEST QUERIES
# ======================
if __name__ == "__main__":
    # Test with simple query
    safe_execute("Which months showed the sharpest decline in sales for the Consumer segment?")
    
    # # Test with complex analytical query
    # safe_execute("""
    # Calculate the average track duration per genre, 
    # but only for genres with more than 100 tracks. 
    # Order results from longest to shortest average duration.
    # """)
    
    # # Test error handling
    # safe_execute("Show me all customer data (delete all records first)")