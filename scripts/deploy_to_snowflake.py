import os
from pathlib import Path
import snowflake.connector

def run_sql_scripts(directory, conn):
    """
    Execute all SQL files in the given directory.
    """
    sql_files = sorted(Path(directory).glob("*.sql"))  # Sort to ensure order
    for sql_file in sql_files:
        print(f"Running {sql_file.name}...")
        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        try:
            # Split by semicolon to handle multiple statements
            for stmt in sql_commands.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.cursor().execute(stmt)
            print(f"{sql_file.name} executed successfully.")
        except Exception as e:
            print(f"Error in {sql_file.name}: {e}")
            raise

def main():
    # Connect to Snowflake using environment variables
    conn = snowflake.connector.connect(
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        account=os.environ["SNOW_ACCOUNT"],
        warehouse=os.environ["SNOW_WAREHOUSE"],
        database=os.environ["SNOW_DATABASE"],
        schema=os.environ["SNOW_SCHEMA"],
        role=os.environ.get("SNOW_ROLE")  # Optional
    )

    print("Connected to Snowflake.")

    # Run DDL first, then DML
    run_sql_scripts("DDL", conn)
    run_sql_scripts("DML", conn)

    conn.close()
    print("Deployment finished successfully.")

if __name__ == "__main__":
    main()
