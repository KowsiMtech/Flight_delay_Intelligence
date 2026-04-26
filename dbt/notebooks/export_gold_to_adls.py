# Notebook: export_gold_to_adls
# Purpose : Export all dbt Gold tables from Databricks → ADLS Gen2 as Parquet
# Run after: dbt run completes successfully

# ── CELL 1: CONFIG ──────────────────────────────────────────────
CATALOG       = 'flight_catalog'
GOLD_SCHEMA   = 'gold_gold'
STORAGE_ACCT  = 'flightanalyticsadls'
CONTAINER     = 'flight-data'
GOLD_PATH     = f'abfss://{CONTAINER}@{STORAGE_ACCT}.dfs.core.windows.net/gold'

# Tables to export
GOLD_TABLES = [
    'dim_airline',
    'dim_airport',
    'fact_flight_delays',
    'fact_delay_cascade',
    'mart_carrier_performance',
    'mart_airport_operations',
    'mart_delay_weather_correlation',
]

print(f'Export destination: {GOLD_PATH}')
print(f'Tables to export  : {len(GOLD_TABLES)}')

# ── CELL 2: EXPORT FUNCTION ─────────────────────────────────────
from datetime import datetime

def export_table_to_adls(table_name, gold_path, mode='overwrite'):
    """
    Reads a Gold Delta table from Unity Catalog
    and writes it to ADLS as Parquet.
    
    Args:
        table_name : Delta table name in gold_gold schema
        gold_path  : ADLS destination base path
        mode       : 'overwrite' replaces existing, 'append' adds to existing
    """
    full_table  = f'{CATALOG}.{GOLD_SCHEMA}.{table_name}'
    output_path = f'{gold_path}/{table_name}/'
    
    print(f'\n{"="*50}')
    print(f'Exporting: {full_table}')
    print(f'        → {output_path}')
    
    try:
        df = spark.table(full_table)
        row_count = df.count()
        
        # Write as Parquet — industry standard for ADLS exports
        (df.write
           .format('parquet')
           .mode(mode)
           .option('compression', 'snappy')   # fast + good compression
           .save(output_path)
        )
        
        print(f'✅ Done — {row_count:,} rows exported')
        return {'table': table_name, 'rows': row_count, 'status': 'success'}
    
    except Exception as e:
        print(f'❌ Failed — {str(e)}')
        return {'table': table_name, 'rows': 0, 'status': 'failed', 'error': str(e)}


# ── CELL 3: RUN EXPORT ──────────────────────────────────────────
print(f'Starting Gold export at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print(f'Destination: {GOLD_PATH}')

results = []
for table in GOLD_TABLES:
    result = export_table_to_adls(table, GOLD_PATH)
    results.append(result)

# ── CELL 4: SUMMARY ─────────────────────────────────────────────
print('\n' + '='*50)
print('EXPORT SUMMARY')
print('='*50)

success = [r for r in results if r['status'] == 'success']
failed  = [r for r in results if r['status'] == 'failed']

for r in results:
    status = '✅' if r['status'] == 'success' else '❌'
    print(f"{status} {r['table']:<40} {r.get('rows', 0):>8,} rows")

print('='*50)
print(f'Total  : {len(results)} tables')
print(f'Success: {len(success)} tables')
print(f'Failed : {len(failed)} tables')
print(f'Finished at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

# ── CELL 5: VERIFY FILES IN ADLS ────────────────────────────────
print('\nVerifying files in ADLS:')
for table in GOLD_TABLES:
    path = f'{GOLD_PATH}/{table}/'
    try:
        files = dbutils.fs.ls(path)
        parquet_files = [f for f in files if f.name.endswith('.parquet') or f.name.startswith('part-')]
        print(f'✅ {table:<40} {len(parquet_files)} parquet files')
    except Exception as e:
        print(f'❌ {table:<40} NOT FOUND')
