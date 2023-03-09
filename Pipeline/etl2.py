import os, luigi, time
from google.cloud import storage, bigquery
import pandas as pd

class LoadToCGS(luigi.Task):
    bucket_name = "dtc_project_bucket"
    file_name_translated = "translated_data.csv"
    file_name_cleaned = "cleaned_french.csv"
    file_name_parquet = "translated_data.parquet"
    local_path_translated = "C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/translated_data.csv"
    local_path_cleaned = "C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/cleaned_french.csv"
    local_path_parquet = "C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/translated_data.parquet"

    def output(self):
        return [luigi.LocalTarget(self.local_path_translated),
                luigi.LocalTarget(self.local_path_cleaned),
                luigi.LocalTarget(self.local_path_parquet)]

    def run(self):
        print(f"Uploading {self.local_path_translated} to GCS bucket {self.bucket_name} as {self.file_name_translated}...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob_translated = bucket.blob(self.file_name_translated)
        blob_translated.upload_from_filename(self.local_path_translated)
        print(f"{self.local_path_translated} uploaded to GCS bucket {self.bucket_name} as {self.file_name_translated}.")

        print(f"Uploading {self.local_path_cleaned} to GCS bucket {self.bucket_name} as {self.file_name_cleaned}...")
        blob_cleaned = bucket.blob(self.file_name_cleaned)
        blob_cleaned.upload_from_filename(self.local_path_cleaned)
        print(f"{self.local_path_cleaned} uploaded to GCS bucket {self.bucket_name} as {self.file_name_cleaned}.")

        print(f"Uploading {self.local_path_parquet} to GCS bucket {self.bucket_name} as {self.file_name_parquet}...")
        df = pd.read_parquet(self.local_path_parquet)
        df.to_csv(self.local_path_parquet, index=False)
        blob_parquet = bucket.blob(self.file_name_parquet)
        blob_parquet.upload_from_filename(self.local_path_parquet)
        print(f"{self.local_path_parquet} uploaded to GCS bucket {self.bucket_name} as {self.file_name_parquet}.")

    def complete(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob_translated = bucket.blob(self.file_name_translated)
        blob_cleaned = bucket.blob(self.file_name_cleaned)
        blob_parquet = bucket.blob(self.file_name_parquet)
        exists_translated = blob_translated.exists()
        exists_cleaned = blob_cleaned.exists()
        exists_parquet = blob_parquet.exists()
        print(f"File {self.file_name_translated} exists in GCS bucket {self.bucket_name}: {exists_translated}")
        print(f"File {self.file_name_cleaned} exists in GCS bucket {self.bucket_name}: {exists_cleaned}")
        print(f"File {self.file_name_parquet} exists in GCS bucket {self.bucket_name}: {exists_parquet}")
        return exists_translated and exists_cleaned and exists_parquet
    time.sleep(5)



class CreateBigQueryDataset(luigi.Task):
    dataset_id = luigi.Parameter(default="DTCP_dataset")
    credentials_path = luigi.Parameter(default="#YOUR CREDENTIALS")

    def requires(self):
        return LoadToCGS()
    
    def output(self):
        return luigi.LocalTarget(self.dataset_id)

    def run(self):
        print(f"Creating BigQuery dataset {self.dataset_id}...")
        client = bigquery.Client.from_service_account_json(self.credentials_path)
        dataset_ref = client.dataset(self.dataset_id)
        print(f"Dataset reference created for {self.dataset_id}.")
        
        if client.get_dataset(dataset_ref):
            print(f"BigQuery dataset {self.dataset_id} already exists. Skipping creation.")
        else:
            dataset = bigquery.Dataset(dataset_ref)
            dataset = client.create_dataset(dataset)
            print(f"Created BigQuery dataset {self.dataset_id}.")
            
        with self.output().open('w') as f:
            f.write('done')
            time.sleep(5)



class CreateRadioactiveWasteTable(luigi.Task):
    dataset_id = luigi.Parameter(default="DTCP_dataset")
    table_id = luigi.Parameter(default="French_Radioactive_Waste")
    credentials_path = luigi.Parameter(default="#YOUR CREDENTIALS")

    def requires(self):
        return CreateBigQueryDataset()
    
    def output(self):
        return luigi.LocalTarget("table_created.txt")

    def run(self):
        print(f"Checking if table {self.table_id} already exists...")
        client = bigquery.Client.from_service_account_json(self.credentials_path)
        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        if client.get_table(table_ref):
            print(f"BigQuery table {self.table_id} already exists. Skipping creation.")
        else:
            print(f"Creating BigQuery table {self.table_id}...")
            table_schema = [
                bigquery.SchemaField("City", "STRING"),
                bigquery.SchemaField("County", "STRING"),
                bigquery.SchemaField("Code", "STRING"),
                bigquery.SchemaField("Site", "STRING"),
                bigquery.SchemaField("Waste_Type", "STRING"),
                bigquery.SchemaField("Waste_Sub_Type", "STRING"),
                bigquery.SchemaField("Description", "STRING"),
                bigquery.SchemaField("Category", "STRING"),
                bigquery.SchemaField("Label", "STRING"),
                bigquery.SchemaField("Volume_in_mt", "STRING"),
                bigquery.SchemaField("Increase", "STRING"),
                bigquery.SchemaField("BQ", "STRING"),
                bigquery.SchemaField("Radionuclides", "STRING")
            ]
            table = bigquery.Table(table_ref, schema=table_schema)
            table = client.create_table(table)
            print(f"Created BigQuery table {self.table_id}.")
            
        with self.output().open('w') as f:
            f.write('done')
            time.sleep(5)

class IngestDataToBigQuery(luigi.Task):

    bucket_name = "dtc_project_bucket"
    file_name = "translated_data.parquet"
    dataset_id = "DTCP_dataset"
    table_id = "French_Radioactive_Waste"
    project_id = "dtc-capstone-project"
    credentials_path = luigi.Parameter(default="#YOUR CREDENTIALS")

    def requires(self):
        return CreateBigQueryDataset()

    def run(self):
        client = bigquery.Client()
        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        table = client.get_table(table_ref)
        if table.num_rows > 0:
            print(f"Table {table.project}.{table.dataset_id}.{table.table_id} already contains {table.num_rows} rows, skipping task")
            return

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.file_name)
        uri = f"gs://{self.bucket_name}/{self.file_name}"
        
        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config)
        load_job.result()  
        
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into {table.project}.{table.dataset_id}.{table.table_id}")

    def output(self):
        return luigi.LocalTarget("data_loaded.txt")
    time.sleep(5)


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "#YOUR CREDENTIALS"
    luigi.run(['IngestDataToBigQuery', '--local-scheduler'])





