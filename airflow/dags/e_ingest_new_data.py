from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging
from io import StringIO

# Configuration
POSTGRES_SCHEMA = 'public'  # Adjust based on your PostgreSQL schema
TRINO_CATALOG = 'iceberg'
BRONZE_SCHEMA = 'bronze'
S3_LOCATION = 's3a://stocks/'
BUCKET_NAME = 'stocks'

# Table configuration
TABLES = [
    {
        'name': 'sp500_companies',
        'source_catalog': 'postgres',
        'source_schema': POSTGRES_SCHEMA,
        'source_table': 'sp500_companies',
        'primary_key': 'symbol',
        'batch_size': 100
    }
]

# Yahoo Finance configuration
FINANCE_TABLE_CONFIG = {
    'name': 'company_finance_data',
    'primary_key': ['symbol', 'date'],
    'batch_size': 1000,
    'lookback_days': 365,
    'data_interval': '1d'
}

# Default DAG arguments
default_args = {
    'owner': 'AshryMan',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def create_iceberg_schema():
    """Create Iceberg schema in Minio"""
    try:
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        # Create schema if not exists
        schema_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{BRONZE_SCHEMA}
            WITH (location = '{S3_LOCATION}')
        """
        trino_hook.run(schema_sql)
        logging.info(f"Schema {TRINO_CATALOG}.{BRONZE_SCHEMA} created/verified")
    except Exception as e:
        logging.error(f"Failed to create schema: {str(e)}")
        raise

def create_postgres_table_structure(table_config):
    """Create Iceberg table structure for PostgreSQL data"""
    table_name = table_config['name']
    source_catalog = table_config['source_catalog']
    source_schema = table_config['source_schema']
    source_table = table_config['source_table']
    
    try:
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        # Get column definitions from PostgreSQL via Trino
        describe_sql = f"DESCRIBE {source_catalog}.{source_schema}.{source_table}"
        columns = trino_hook.get_records(describe_sql)
        
        # Build CREATE TABLE statement
        column_defs = []
        for col in columns:
            col_name = col[0]
            col_type = col[1].upper()
            
            # Map PostgreSQL types to Iceberg compatible types if needed
            if 'VARCHAR' in col_type or 'TEXT' in col_type:
                col_type = 'VARCHAR'
            elif 'INTEGER' in col_type or 'INT4' in col_type:
                col_type = 'INTEGER'
            elif 'BIGINT' in col_type or 'INT8' in col_type:
                col_type = 'BIGINT'
            elif 'NUMERIC' in col_type or 'DECIMAL' in col_type:
                col_type = 'DECIMAL(20,6)'
            elif 'TIMESTAMP' in col_type:
                col_type = 'TIMESTAMP'
            elif 'DATE' in col_type:
                col_type = 'DATE'
            elif 'BOOLEAN' in col_type or 'BOOL' in col_type:
                col_type = 'BOOLEAN'
            elif 'DOUBLE' in col_type or 'FLOAT8' in col_type:
                col_type = 'DOUBLE'
            elif 'REAL' in col_type or 'FLOAT4' in col_type:
                col_type = 'REAL'
                
            column_defs.append(f'"{col_name}" {col_type}')
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{BRONZE_SCHEMA}.{table_name} (
                {', '.join(column_defs)},
                load_timestamp TIMESTAMP
            )
        """
        
        trino_hook.run(create_sql)
        logging.info(f"Table {TRINO_CATALOG}.{BRONZE_SCHEMA}.{table_name} created/verified")
        
    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {str(e)}")
        raise

def copy_postgres_to_iceberg(table_config):
    """Copy data from PostgreSQL to Iceberg table"""
    table_name = table_config['name']
    source_catalog = table_config['source_catalog']
    source_schema = table_config['source_schema']
    source_table = table_config['source_table']
    primary_key = table_config['primary_key']
    
    try:
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        # Get column names from source table
        describe_sql = f"DESCRIBE {source_catalog}.{source_schema}.{source_table}"
        columns = trino_hook.get_records(describe_sql)
        column_names = [col[0] for col in columns]
        
        # Prepare column lists
        source_columns = ', '.join([f'"{col}"' for col in column_names])
        target_columns = ', '.join([f'"{col}"' for col in column_names] + ['load_timestamp'])
        
        # Generate update clause (exclude primary key)
        update_clause = ", ".join(
            [f'"{col}" = source."{col}"' for col in column_names if col != primary_key]
        )
        
        # Generate insert values
        insert_values = ", ".join([f'source."{col}"' for col in column_names] + ['CURRENT_TIMESTAMP'])
        
        # Merge/Insert data
        merge_sql = f"""
            MERGE INTO {TRINO_CATALOG}.{BRONZE_SCHEMA}.{table_name} AS target
            USING (
                SELECT {source_columns}
                FROM {source_catalog}.{source_schema}.{source_table}
            ) AS source
            ON target."{primary_key}" = source."{primary_key}"
            WHEN MATCHED THEN 
                UPDATE SET {update_clause}, load_timestamp = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT ({target_columns}) VALUES ({insert_values})
        """
        
        trino_hook.run(merge_sql)
        logging.info(f"Successfully copied data to {TRINO_CATALOG}.{BRONZE_SCHEMA}.{table_name}")
        
    except Exception as e:
        logging.error(f"Failed to copy data for {table_name}: {str(e)}")
        raise

def get_top_companies():
    """Get top 10 companies from the S&P 500 list stored in Iceberg"""
    try:
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        # Query to get top 10 companies (adjust criteria as needed)
        query = f"""
            SELECT symbol, shortname
            FROM {TRINO_CATALOG}.{BRONZE_SCHEMA}.sp500_companies
            order by marketcap DESC
            LIMIT 10
        """
        
        results = trino_hook.get_records(query)
        
        if not results:
            raise ValueError("No companies found in sp500_companies table")
        
        top_companies = [
            {'symbol': row[0], 'company_name': row[1]}
            for row in results
        ]
        
        logging.info(f"Retrieved top 10 companies: {[c['symbol'] for c in top_companies]}")
        return top_companies
        
    except Exception as e:
        logging.error(f"Failed to get top companies: {str(e)}")
        raise

def create_finance_table_structure():
    """Create Iceberg table for finance data - Optimized version"""
    try:
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{BRONZE_SCHEMA}.{FINANCE_TABLE_CONFIG['name']} (
                symbol VARCHAR,
                date DATE,
                open DECIMAL(20,6),
                high DECIMAL(20,6),
                low DECIMAL(20,6),
                close DECIMAL(20,6),
                volume BIGINT,
                dividends DECIMAL(20,6),
                stock_splits DECIMAL(20,6),
                load_timestamp TIMESTAMP
            )
            WITH (
                partitioning = ARRAY['symbol', 'year(date)'],
                format = 'PARQUET'
            )
        """
        
        trino_hook.run(create_sql)
        logging.info(f"Finance data table {FINANCE_TABLE_CONFIG['name']} created/verified")
        
    except Exception as e:
        logging.error(f"Failed to create finance table: {str(e)}")
        raise

def fetch_yfinance_data(**context):
    """Fetch financial data from Yahoo Finance for top companies - Optimized with parallel processing"""
    try:
        # Get top companies from XCom
        ti = context['ti']
        top_companies = ti.xcom_pull(task_ids='get_top_companies')
        
        if not top_companies:
            raise ValueError("No companies received from previous task")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=FINANCE_TABLE_CONFIG['lookback_days'])
        
        symbols = [company['symbol'] for company in top_companies]
        
        try:
            # Use yfinance's download method for bulk data fetching (much faster)
            data = yf.download(
                tickers=symbols,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval=FINANCE_TABLE_CONFIG['data_interval'],
                group_by='ticker',
                progress=False,
                threads=True  # Enable threading for faster downloads
            )
            
            if data.empty:
                logging.warning("No data found for any symbol")
                return None
            
            all_data = []
            
            # Process each symbol's data
            for symbol in symbols:
                # Check if we have data for this symbol
                if symbol in data.columns.get_level_values(0):
                    symbol_data = data[symbol].copy()
                    
                    if not symbol_data.empty:
                        # Reset index to get Date as column
                        symbol_data = symbol_data.reset_index()
                        symbol_data['symbol'] = symbol
                        symbol_data['load_timestamp'] = datetime.now()
                        
                        # Rename columns to match our table schema
                        symbol_data = symbol_data.rename(columns={
                            'Date': 'date',
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume',
                            'Dividends': 'dividends',
                            'Stock Splits': 'stock_splits'
                        })
                        
                        # Select only columns we need
                        symbol_data = symbol_data[[
                            'symbol', 'date', 'open', 'high', 'low', 'close', 
                            'volume', 'dividends', 'stock_splits', 'load_timestamp'
                        ]]
                        
                        all_data.append(symbol_data)
                        logging.info(f"Fetched {len(symbol_data)} records for {symbol}")
                    else:
                        logging.warning(f"No data found for {symbol}")
                else:
                    logging.warning(f"No data available for {symbol}")
            
            if not all_data:
                raise ValueError("No data fetched for any company")
            
            # Combine all company data
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Sort by symbol and date for better partitioning
            combined_df = combined_df.sort_values(['symbol', 'date'])
            
            # Convert to CSV for Trino ingestion
            csv_buffer = StringIO()
            combined_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Store in XCom for next task
            ti.xcom_push(key='finance_data_csv', value=csv_data)
            ti.xcom_push(key='finance_data_records', value=len(combined_df))
            
            logging.info(f"Successfully fetched {len(combined_df)} records for {len(all_data)} companies")
            
        except Exception as e:
            logging.error(f"Failed to bulk download data: {str(e)}")
            # Fallback to individual downloads
            return fetch_yfinance_data_individually(top_companies, start_date, end_date, ti)
            
    except Exception as e:
        logging.error(f"Failed to fetch yfinance data: {str(e)}")
        raise

def fetch_yfinance_data_individually(companies, start_date, end_date, ti):
    """Fallback method for individual company data fetching"""
    all_data = []
    
    for company in companies:
        symbol = company['symbol']
        
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d'
            )
            
            if hist.empty:
                logging.warning(f"No data found for {symbol}")
                continue
            
            hist = hist.reset_index()
            hist['symbol'] = symbol
            hist['load_timestamp'] = datetime.now()
            
            hist = hist.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Dividends': 'dividends',
                'Stock Splits': 'stock_splits'
            })
            
            hist = hist[[
                'symbol', 'date', 'open', 'high', 'low', 'close', 
                'volume', 'dividends', 'stock_splits', 'load_timestamp'
            ]]
            
            all_data.append(hist)
            logging.info(f"Fetched {len(hist)} records for {symbol}")
            
        except Exception as e:
            logging.error(f"Failed to fetch data for {symbol}: {str(e)}")
            continue
    
    if not all_data:
        raise ValueError("No data fetched for any company")
    
    combined_df = pd.concat(all_data, ignore_index=True)
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    ti.xcom_push(key='finance_data_csv', value=csv_data)
    ti.xcom_push(key='finance_data_records', value=len(combined_df))
    
    return combined_df

def load_finance_data_to_iceberg(**context):
    """Load fetched finance data into Iceberg table - Fixed version"""
    try:
        ti = context['ti']
        csv_data = ti.xcom_pull(task_ids='fetch_yfinance_data', key='finance_data_csv')
        
        if not csv_data:
            raise ValueError("No finance data received from previous task")
        
        trino_hook = TrinoHook(trino_conn_id='trino_conn')
        
        # Parse CSV and create bulk INSERT with VALUES
        lines = csv_data.strip().split('\n')
        headers = [h.strip('"') for h in lines[0].split(',')]
        
        # Process in large batches
        BATCH_SIZE = FINANCE_TABLE_CONFIG['batch_size']
        all_values = []
        
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if not line:
                continue
                
            values = line.split(',')
            if len(values) != len(headers):
                continue
                
            # Convert values to proper format
            symbol = values[headers.index('symbol')].strip()
            date_val = values[headers.index('date')].strip()
            
            # Handle numeric values - convert empty strings to NULL
            def format_numeric(value):
                value = value.strip()
                if value == '' or value == 'None':
                    return 'NULL'
                try:
                    # Ensure the value can be parsed as float
                    float(value)
                    return value
                except:
                    return 'NULL'
            
            open_val = format_numeric(values[headers.index('open')])
            high_val = format_numeric(values[headers.index('high')])
            low_val = format_numeric(values[headers.index('low')])
            close_val = format_numeric(values[headers.index('close')])
            volume_val = format_numeric(values[headers.index('volume')])
            dividends_val = format_numeric(values[headers.index('dividends')])
            stock_splits_val = format_numeric(values[headers.index('stock_splits')])
            
            # Format date and timestamp
            # Extract just the date part (YYYY-MM-DD)
            date_parts = date_val.split(' ')[0]
            load_timestamp = values[headers.index('load_timestamp')].strip()
            
            # Create properly formatted value tuple
            value_str = f"""(
                '{symbol.replace("'", "''")}',
                DATE '{date_parts}',
                {open_val},
                {high_val},
                {low_val},
                {close_val},
                {volume_val},
                {dividends_val},
                {stock_splits_val},
                TIMESTAMP '{load_timestamp}'
            )"""
            all_values.append(value_str)
        
        # Insert in batches
        for batch_idx in range(0, len(all_values), BATCH_SIZE):
            batch = all_values[batch_idx:batch_idx + BATCH_SIZE]
            
            # Create staging table for this batch
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            staging_table = f"staging_finance_{timestamp}_{batch_idx}"
            
            # Create staging table with proper type casting
            # Instead of using NULLIF, we'll handle NULLs in the VALUES clause
            create_staging_sql = f"""
                CREATE TABLE {TRINO_CATALOG}.{BRONZE_SCHEMA}.{staging_table}
                WITH (format = 'PARQUET') AS
                SELECT 
                    CAST(symbol AS VARCHAR) as symbol,
                    CAST(trading_date AS DATE) as date,
                    CAST(open_price AS DECIMAL(20,6)) as open,
                    CAST(high_price AS DECIMAL(20,6)) as high,
                    CAST(low_price AS DECIMAL(20,6)) as low,
                    CAST(close_price AS DECIMAL(20,6)) as close,
                    CAST(trading_volume AS BIGINT) as volume,
                    CAST(dividend_amount AS DECIMAL(20,6)) as dividends,
                    CAST(stock_split AS DECIMAL(20,6)) as stock_splits,
                    CAST(load_time AS TIMESTAMP) as load_timestamp
                FROM (VALUES {','.join(batch)}) AS t(
                    symbol, trading_date, open_price, high_price, low_price, close_price,
                    trading_volume, dividend_amount, stock_split, load_time
                )
            """
            
            trino_hook.run(create_staging_sql)
            
            # Merge data using efficient bulk merge
            primary_keys = FINANCE_TABLE_CONFIG['primary_key']
            pk_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
            
            column_names = [
                'symbol', 'date', 'open', 'high', 'low', 'close', 
                'volume', 'dividends', 'stock_splits', 'load_timestamp'
            ]
            
            update_columns = [col for col in column_names if col not in primary_keys]
            update_clause = ", ".join([f"{col} = source.{col}" for col in update_columns])
            
            insert_columns = ", ".join(column_names)
            insert_values = ", ".join([f"source.{col}" for col in column_names])
            
            merge_sql = f"""
                MERGE INTO {TRINO_CATALOG}.{BRONZE_SCHEMA}.{FINANCE_TABLE_CONFIG['name']} AS target
                USING {TRINO_CATALOG}.{BRONZE_SCHEMA}.{staging_table} AS source
                ON {pk_condition}
                WHEN MATCHED THEN 
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns}) VALUES ({insert_values})
            """
            
            trino_hook.run(merge_sql)
            
            # Drop staging table
            drop_sql = f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{BRONZE_SCHEMA}.{staging_table}"
            trino_hook.run(drop_sql)
            
            logging.info(f"Processed batch {batch_idx//BATCH_SIZE + 1}/{(len(all_values) + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        logging.info(f"Successfully loaded finance data to {FINANCE_TABLE_CONFIG['name']}")
        
    except Exception as e:
        logging.error(f"Failed to load finance data: {str(e)}")
        raise

# Define the DAG
with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    tags=['stocks', 'postgresql', 'yfinance', 'trino', 'iceberg', 'minio'],
    is_paused_upon_creation=False,
) as dag:

    # Task 1: Create Iceberg schema
    create_schema_task = PythonOperator(
        task_id='create_iceberg_schema',
        python_callable=create_iceberg_schema,
    )

    # Task 2: Create PostgreSQL table structure in Iceberg
    create_postgres_table_task = PythonOperator(
        task_id='create_postgres_table_structure',
        python_callable=create_postgres_table_structure,
        op_kwargs={'table_config': TABLES[0]},
    )

    # Task 3: Copy PostgreSQL data to Iceberg
    copy_postgres_task = PythonOperator(
        task_id='copy_postgres_to_iceberg',
        python_callable=copy_postgres_to_iceberg,
        op_kwargs={'table_config': TABLES[0]},
    )

    # Task 4: Get top 10 companies
    get_top_companies_task = PythonOperator(
        task_id='get_top_companies',
        python_callable=get_top_companies,
    )

    # Task 5: Create finance data table structure (optimized)
    create_finance_table_task = PythonOperator(
        task_id='create_finance_table_structure',
        python_callable=create_finance_table_structure,
    )

    # Task 6: Fetch Yahoo Finance data (optimized with bulk download)
    fetch_yfinance_task = PythonOperator(
        task_id='fetch_yfinance_data',
        python_callable=fetch_yfinance_data,
    )

    # Task 7: Load finance data to Iceberg (optimized with CTAS and batch processing)
    load_finance_task = PythonOperator(
        task_id='load_finance_data_to_iceberg',
        python_callable=load_finance_data_to_iceberg,
    )

    # Set task dependencies
    create_schema_task >> create_postgres_table_task >> copy_postgres_task >> get_top_companies_task >> create_finance_table_task >> fetch_yfinance_task >> load_finance_task