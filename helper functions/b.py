import psycopg2

def get_databases():
    try:
        # Connect to PostgreSQL (default DB is usually 'postgres')
        conn = psycopg2.connect(
            dbname="postgres",   # system DB to connect
            user="postgres",
            password="moni123",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Query to list databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        
        databases = cursor.fetchall()
        
        print("Databases:")
        for db in databases:
            print("-", db[0])

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    get_databases()
