import psycopg2
import os

run_locally = True

class Connection:
    """Small psycopg2 wrapper for executing SQL and fetching results (optionally as a DataFrame)."""
    if run_locally:
        db_host = "localhost"
        db_port = "5433"
    else:
        db_host = os.getenv("DB_HOST", "db")
        db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "vejr")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
    conn = psycopg2.connect(
        host=db_host,
        port=int(db_port),  # Cast to int since env vars are strings
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    cur = conn.cursor()

    def execute(self, query_str: str, args=None):
        """Execute SQL; on error prints the exception and rolls back."""
        try:
            self.cur.execute(query_str,args)
        except Exception as e:
            print("Execution failed:")
            print(e)
            self.conn.rollback()

    def query_fetch(self, query_str: str):
        """Run a query and return all rows as a list of tuples."""
        self.execute(query_str)
        return self.cur.fetchall()

    # def query_fetch_df(self, query_str: str):
    #     """Run a query and return as a pandas DataFrame."""
    #     return pd.DataFrame(self.query_fetch(query_str), columns=[desc.name for desc in self.cur.description])

    def commit(self):
        """Commit the current transaction."""
        self.conn.commit()

    def close(self, commit=True):
        """Close the database connection."""
        if commit:
            self.commit()
        self.conn.close()

    # def table_exists(self, table_name: str):
    #     """Return True if `table_name` exists, otherwise False (rolls back on missing table)."""
    #     try:
    #         self.cur.execute(f'SELECT * FROM {table_name} LIMIT 1')
    #     except psycopg2.errors.UndefinedTable:
    #         self.conn.rollback()
    #         return False
    #     return True