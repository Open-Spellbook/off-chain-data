import os
import json
import requests
import time  # import the time module
from google.cloud import bigquery

# Record start time for the entire script
script_start_time = time.time()


# Initialize BigQuery client
credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
client = bigquery.Client()

def create_table():
    start_time = time.time()  # Record the start time for create_table
    dataset_id = 'cowswap'
    table_id = 'raw_app_data'

    schema = [
        bigquery.SchemaField('app_hash', 'STRING'),
        bigquery.SchemaField('content', 'RECORD', fields=[
            bigquery.SchemaField('appCode', 'STRING'),
            bigquery.SchemaField('environment', 'STRING'),
            bigquery.SchemaField('fullAppData', 'STRING'),
            bigquery.SchemaField('metadata', 'RECORD', fields=[
                bigquery.SchemaField('environment', 'STRING'),
                bigquery.SchemaField('orderClass', 'RECORD', fields=[
                    bigquery.SchemaField('orderClass', 'STRING'),
                    bigquery.SchemaField('version', 'STRING')
                ]),
                bigquery.SchemaField('quote', 'RECORD', fields=[
                    bigquery.SchemaField('buyAmount', 'STRING'),
                    bigquery.SchemaField('sellAmount', 'STRING'),
                    bigquery.SchemaField('slippageBips', 'STRING'),
                    bigquery.SchemaField('version', 'STRING')
                ]),
                bigquery.SchemaField('referrer', 'RECORD', fields=[
                    bigquery.SchemaField('address', 'STRING'),
                    bigquery.SchemaField('kind', 'STRING'),
                    bigquery.SchemaField('referrer', 'STRING'),
                    bigquery.SchemaField('version', 'STRING')
                ]),
                bigquery.SchemaField('utm', 'RECORD', fields=[
                    bigquery.SchemaField('utmCampaign', 'STRING'),
                    bigquery.SchemaField('utmContent', 'STRING'),
                    bigquery.SchemaField('utmMedium', 'STRING'),
                    bigquery.SchemaField('utmSource', 'STRING'),
                    bigquery.SchemaField('utmTerm', 'STRING'),
                    bigquery.SchemaField('version', 'STRING')
                ])
            ]),
            bigquery.SchemaField('version', 'STRING')
        ]),
        bigquery.SchemaField('first_seen_block', 'INT64')
    ]

    datasets = list(client.list_datasets())
    dataset_exists = any(dataset.dataset_id == dataset_id for dataset in datasets)

    if not dataset_exists:
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref)

    # Check if the table exists within the dataset
    tables = list(client.list_tables(dataset_id))
    table_exists = any(table.table_id == table_id for table in tables)

    # Delete the table if it exists
    if table_exists:
        table_ref = client.dataset(dataset_id).table(table_id)
        client.delete_table(table_ref)

    # Create a new table
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

    end_time = time.time()  # Record the end time for create_table
    return end_time - start_time  # Return the elapsed time

def fetch_and_insert_into_bigquery(hash_list):
    start_time = time.time()  # Record the start time for fetch_and_insert_into_bigquery

    base_url = "https://api.cow.fi/mainnet/api/v1/app_data/"
    rows_to_insert = []
    fail_count = 0

    
    for hash_entry in hash_list:
        hash_value = hash_entry['hash']
        first_seen_block = hash_entry['first_seen_block']
        
        url = f"{base_url}{hash_value}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = json.loads(response.text)['fullAppData']
                parsed_data = json.loads(data)

                row = {
                    "app_hash": hash_value,
                    "content": {
                        "appCode": parsed_data.get("appCode", ""),
                        "environment": parsed_data.get("environment", ""),
                        "metadata": parsed_data.get("metadata", {})
                    },
                    "first_seen_block": first_seen_block
                }
                
                rows_to_insert.append(row)
                print(f"Successfully fetched data for hash {hash_value}. count = {len(rows_to_insert)}")
                
            else:
                fail_count += 1
                print(f"Fail count: {fail_count} Failed to fetch data for hash {hash_value}. Status code: {response.status_code}")
                
        except Exception as e:
            print(f"An error occurred: {e}")

    table_ref = client.dataset('cowswap').table('raw_app_data')
    table = client.get_table(table_ref)
    
    for row in rows_to_insert:
        errors = client.insert_rows_json(table, [row])
        if errors:
            print(f"Errors occurred for row {row}: {errors}")
        else:
            print(f"Row successfully inserted: {row}")

    end_time = time.time()  # Record the end time for fetch_and_insert_into_bigquery
    return end_time - start_time  # Return the elapsed time

if __name__ == "__main__":
    # Record start time for SQL query
    query_start_time = time.time()
    
    create_table_time = create_table()
    
    sql_query = "SELECT trades, call_block_number FROM gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle WHERE call_success = True"
    query_job = client.query(sql_query)
    
    # Record end time for SQL query
    query_end_time = time.time()
    
    hash_dict = {}  # Store hash and the earliest first_seen_block
    count = 0
    for row in query_job:
        trades_ = row.trades
        call_block_number = row.call_block_number
        
        for trade in trades_:
            app_data_hash = trade.split(',')
            
            if len(app_data_hash) > 6:
                hash_value = app_data_hash[6]
                if hash_value != "0x0000000000000000000000000000000000000000000000000000000000000001":
                    # Update first_seen_block only if it's smaller than the existing one
                    if hash_value not in hash_dict or call_block_number < hash_dict[hash_value]:
                        hash_dict[hash_value] = call_block_number
                        print(count)
                        count += 1

    hash_list = [{"hash": k, "first_seen_block": v} for k, v in hash_dict.items()]
    
    fetch_and_insert_time = fetch_and_insert_into_bigquery(hash_list)
    
    # Record end time for the entire script
    script_end_time = time.time()

    # Print out the time taken for each stage
    print(f"Time taken for create_table: {create_table_time} seconds")
    print(f"Time taken for SQL query: {query_end_time - query_start_time} seconds")
    print(f"Time taken for fetch_and_insert_into_bigquery: {fetch_and_insert_time} seconds")
    print(f"Total script time: {script_end_time - script_start_time} seconds")