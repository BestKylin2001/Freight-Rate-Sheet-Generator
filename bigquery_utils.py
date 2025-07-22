from google.cloud import bigquery

def get_bigquery_client():
    return bigquery.Client()

def get_cleaned_tables(dataset_name: str, prefix: str = "cleaned_"):
    client = get_bigquery_client()
    tables = client.list_tables(dataset_name)
    return [table.table_id for table in tables if table.table_id.startswith(prefix)]