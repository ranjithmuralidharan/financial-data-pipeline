import duckdb
import logging
import os

# Setup logging for production observability
logging.basicConfig(
    filename='pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_warehouse():
    try:
        # Connect to DuckDB (creates banking.db if not exists)
        conn = duckdb.connect("banking.db")
        
        # 1. Ingest clean data into a temporary staging table
        conn.execute("CREATE OR REPLACE TABLE staging_area AS SELECT * FROM read_csv_auto('data/good_transactions.csv')")
        
        # 2. Deduplication logic using ROW_NUMBER (The "Golden Record" pattern)
        # This handles the ID duplicates generated in the first script
        dedup_sql = """
        CREATE OR REPLACE TABLE final_warehouse AS
        WITH ranked_data AS (
            SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_date DESC) as rank
            FROM staging_area
        )
        SELECT * EXCLUDE (rank) FROM ranked_data WHERE rank = 1
        """
        conn.execute(dedup_sql)
        
        # 3. Load the quarantined data into an Audit table
        conn.execute("CREATE OR REPLACE TABLE failed_audit AS SELECT * FROM read_csv_auto('data/bad_transactions.csv')")
        
        logging.info("Warehouse update successful. Deduplication applied.")
        print("🏆 Warehouse logic executed successfully. Check final_warehouse table in banking.db")
        
    except Exception as e:
        logging.error(f"Warehouse Step Failed: {e}")
        raise e

if __name__ == "__main__":
    run_warehouse()
