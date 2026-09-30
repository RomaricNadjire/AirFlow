from pathlib import Path

import pandas as pd
import pendulum

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


BASE_DIR = Path(
    "/home/romaric/Documents/training/AirFlow/training_project"
)

RAW_DIR = BASE_DIR / "data/raw"
PROCESSED_DIR = BASE_DIR / "data/processed"


@dag(
    dag_id="ecommerce_transactions",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["training", "data-engineering"],
    max_active_tasks=4,
    max_active_runs=1
)
def ecommerce_pipeline():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_transactions_table",
        conn_id="postgres_ecommerce",
        sql="""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                country VARCHAR(2) NOT NULL,
                timestamp TIMESTAMP NOT NULL
            );
        """,
    )
   
    create_processed_files_table = SQLExecuteQueryOperator(
        task_id="create_processed_files_table",
        conn_id="postgres_ecommerce",
        sql="""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                rows_processed INTEGER NOT NULL
            );
        """,
    )
    
    # Add source_file to transaction if not already present
    alter_transactions_table = SQLExecuteQueryOperator(
        task_id="alter_transactions_table",
        conn_id="postgres_ecommerce",
        sql="""
            ALTER TABLE transactions
            ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);
        """,
    )
    
    @task
    def get_unprocessed_files():

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        result = hook.get_records(
            """
            SELECT filename
            FROM processed_files
            WHERE status = 'SUCCESS';
            """
        )

        processed = {row[0] for row in result}

        files = sorted(
            RAW_DIR.glob("*.csv")
        )

        unprocessed = [
            file
            for file in files
            if file.name not in processed
        ]

        print(f"Total files found: {len(files)}")
        print(f"Already processed: {len(processed)}")
        print(f"Files to process: {len(unprocessed)}")

        return [str(file) for file in unprocessed]
        
    @task
    def check_file(filename):
        if not Path(filename).exists():
            raise FileNotFoundError(
                f"Input file not found: {filename}"
            )

        print(f"Input file found: {filename}")

    @task(max_active_tis_per_dag=3)
    def process_file(file_path):
        
        import time
        print(f"Starting {file_path}")
        time.sleep(20)

        file_path = Path(file_path)

        df = pd.read_csv(file_path)

        print(
            f"Processing {file_path.name} "
            f"with {len(df)} rows"
        )

        df = df.drop_duplicates()
        df = df.dropna()
        df = df[df["amount"] > 0]

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO transactions (
                transaction_id,
                customer_id,
                product_id,
                amount,
                country,
                timestamp,
                source_file
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                amount = EXCLUDED.amount,
                country = EXCLUDED.country,
                timestamp = EXCLUDED.timestamp,
                source_file = EXCLUDED.source_file;
        """

        for row in df.itertuples(index=False):

            cursor.execute(
                sql,
                (
                    int(row.transaction_id),
                    int(row.customer_id),
                    int(row.product_id),
                    float(row.amount),
                    row.country,
                    row.timestamp,
                    str(file_path).split('/')[-1],
                ),
            )

        conn.commit()

        cursor.close()
        conn.close()

        return {
            "filename": file_path.name,
            "rows_processed": len(df),
        }
    
    @task
    def quality_check(process_result):
        
        if process_result["rows_processed"] == 0:
            print(f"No new rows processed. Quality check skipped for {process_result['filename']}")
            return True
        

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        result = hook.get_first(
            """
            SELECT
                COUNT(*),
                MIN(amount),
                MAX(amount)
            FROM transactions
            WHERE source_file = %s;
            """,
            parameters=(process_result["filename"],)
        )

        total_rows, min_amount, max_amount = result

        print(f"Total rows: {total_rows}")
        print(f"Minimum amount: {min_amount}")
        print(f"Maximum amount: {max_amount}")

        if total_rows == 0:
            raise ValueError(f"Quality check failed: table is empty. Concerned file: {process_result['filename']}")

        if min_amount <= 0:
            raise ValueError(
                "Quality check failed: invalid amount detected."
            )

        print(
            f"Quality check passed for {process_result['rows_processed']} processed rows."
        )
        
        return True
    
    @task
    def mark_file_processed(result):

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        hook.run(
            """
            INSERT INTO processed_files (
                filename,
                status,
                rows_processed
            )
            VALUES (%s, 'SUCCESS', %s)
            ON CONFLICT (filename)
            DO UPDATE SET
                status = EXCLUDED.status,
                rows_processed = EXCLUDED.rows_processed,
                processed_at = CURRENT_TIMESTAMP;
            """,
            parameters=(
                result["filename"],
                result["rows_processed"],
            ),
        )

        print(
            f"Marked {result['filename']} as processed."
        )    
   
    files = get_unprocessed_files()

    processed = process_file.expand(
        file_path=files
    )
        
    check = check_file.expand(
        filename=files
    )
    
    quality = quality_check.expand(
        process_result=processed
    )

    mark_processed = mark_file_processed.expand(
        result=processed
    )
    
    create_table >> alter_transactions_table >> files
    create_processed_files_table >> files
    files >> check >> processed >> quality >> mark_processed


ecommerce_pipeline()