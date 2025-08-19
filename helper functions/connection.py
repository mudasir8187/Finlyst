from langchain.chat_models import init_chat_model
import os
import psycopg2  # Required for PostgreSQL connection
from langchain_community.utilities import SQLDatabase 
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent

load_dotenv()

# ⚠️ WARNING: Hardcoding API keys is INSECURE for production
# For development only! Use environment variables in production
#OPENAI_API_KEY = "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"  # REPLACE THIS WITH YOUR KEY

# # Initialize LLM with direct API key
# llm = init_chat_model(
#     "gpt-4-turbo",  # Using standard model name (gpt-4.1 doesn't exist)
#     api_key=OPENAI_API_KEY
# )

llm = ChatOpenAI(
    model="openai/gpt-4-turbo",  # Note: OpenRouter uses model names like "openai/gpt-4"
    temperature=0,
    openai_api_key=os.getenv("OPENROUTER_API_KEY"),  # Using OpenRouter API key
    openai_api_base="https://openrouter.ai/api/v1"  # OpenRouter's API endpoint
)

# Configure PostgreSQL connection using environment variables

db_user = os.getenv("db_user")     
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")             
db_port = os.getenv("db_port")                   
db_name = os.getenv("db_name")               

# Construct the database URI
db_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Verify connection (optional but recommended)
try:
    # Initialize SQLDatabase connection
    db = SQLDatabase.from_uri(db_uri)
    
    # # 1. Check database dialect
    # print(f"\n✅ Connected to: {db.dialect.upper()} database")
    
    # # 2. Get and count tables
    # tables = db.get_usable_table_names()
    # print(f"📊 Total tables: {len(tables)}")
    # print(f"📋 Available tables: {', '.join(tables)}\n")
    
    # # 3. Run sample query (PostgreSQL requires quotes for case-sensitive names)
    # # NOTE: Chinook uses quoted identifiers ("Artist" not artist)
    # sample_query = 'SELECT * FROM "erp1" LIMIT 5;'
    
    # print(f"🔍 Running query: {sample_query}")
    # results = db.run(sample_query)
    
    # # 4. Pretty print results
    # print("\n🎯 Query Results:")
    # print("-" * 50)
    # print(results)
    # print("-" * 50)
   
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    tools = toolkit.get_tools()

    # for tool in tools:
    #     print(f"{tool.name}: {tool.description}\n")
    

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

    agent = create_react_agent(
        llm,
        tools,
        prompt=system_prompt,
    )

    question = "Identify the top 5 country-product pairs with highest profit volatility."

    for step in agent.stream(
        {"messages": [{"role": "user", "content": question}]},
        stream_mode="values",
    ):
        step["messages"][-1].pretty_print()

except Exception as e:
    print(f"❌ Database operation failed: {str(e)}")
    print("Possible issues:")
    print("1. Incorrect table name casing (PostgreSQL is case-sensitive)")
    print("2. Missing 'langchain-community' package")
    print("3. Database permissions issue")