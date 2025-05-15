from typing import List, Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import re
import numpy as np
from datetime import datetime
import logging

from ..utils import (
    UploadError,
    log_operation,
    log_error,
    retry_with_backoff
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_column_name(name: str) -> str:
    """
    Clean column name to be BigQuery compatible.
    - Replace spaces and special characters with underscores
    - Remove parentheses
    - Convert to lowercase
    """
    # Replace spaces and special characters with underscores
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Convert to lowercase
    name = name.lower()
    return name

def convert_data_types(df: pd.DataFrame, table_schema: List[bigquery.SchemaField]) -> pd.DataFrame:
    """
    Convert DataFrame columns to match BigQuery schema exactly.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Create a mapping of column names to their BigQuery types
    schema_map = {field.name: field.field_type for field in table_schema}
    
    # Log original data types
    logger.info("Original data types:")
    logger.info(df.dtypes)
    
    # Convert each column according to its BigQuery type
    for col in df.columns:
        if col in schema_map:
            bq_type = schema_map[col]
            try:
                if bq_type == 'INTEGER':
                    # First convert to float to handle any string numbers
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Then convert to Int64, replacing inf/-inf/NaN with 0
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0).astype('Int64')
                elif bq_type == 'FLOAT':
                    # First convert to string to handle any non-numeric values
                    df[col] = df[col].astype(str)
                    # Replace any non-numeric strings with empty string
                    df[col] = df[col].replace(['nan', 'None', 'null', 'NaN', 'NULL'], '')
                    # Convert to numeric, replacing any non-numeric values with NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Replace inf/-inf with None
                    df[col] = df[col].replace([np.inf, -np.inf], None)
                    # Replace NaN with None
                    df[col] = df[col].where(pd.notnull(df[col]), None)
                elif bq_type == 'DATETIME':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Replace NaT with None
                    df[col] = df[col].where(pd.notnull(df[col]), None)
                elif bq_type == 'STRING':
                    # Convert to string, replacing NaN with empty string
                    df[col] = df[col].fillna('').astype(str)
                    # Replace 'nan' strings with empty string
                    df[col] = df[col].replace('nan', '')
                elif bq_type == 'BOOLEAN':
                    # Convert to boolean, replacing NaN with False
                    df[col] = df[col].fillna(False).astype(bool)
                logger.info(f"Converted {col} to {bq_type}")
            except Exception as e:
                logger.warning(f"Failed to convert {col} to {bq_type}: {str(e)}")
                # If conversion fails, try to handle it based on type
                if bq_type == 'INTEGER':
                    df[col] = 0
                elif bq_type == 'FLOAT':
                    df[col] = None
                elif bq_type == 'STRING':
                    df[col] = ''
                elif bq_type == 'BOOLEAN':
                    df[col] = False
    
    # Replace NaN with None for better BigQuery compatibility
    df = df.replace({np.nan: None})
    
    # Log final data types
    logger.info("Final data types:")
    logger.info(df.dtypes)
    
    return df

class BigQueryService:
    """Service for handling BigQuery operations"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        # Initialize BigQuery client with just the project ID
        self.client = bigquery.Client(project=project_id)
        
    def create_dataset_if_not_exists(self, dataset_name: str) -> None:
        """
        Create BigQuery dataset if it doesn't exist.
        
        Args:
            dataset_name: Name of the dataset to create
        """
        try:
            dataset_id = f"{self.project_id}.{dataset_name}"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # Set the location
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create dataset {dataset_id}: {str(e)}")
            raise UploadError(f"Failed to create dataset: {str(e)}")

    @retry_with_backoff(max_retries=3)
    def upload_data(
        self,
        dataset_name: str,
        table_name: str,
        data: pd.DataFrame,
        email_name_search_key: str
    ) -> None:
        """
        Upload data to BigQuery table.
        
        Args:
            dataset_name: Name of the BigQuery dataset
            table_name: Name of the BigQuery table
            data: DataFrame containing the data to upload
            email_name_search_key: Email search key for logging
            
        Raises:
            UploadError: If upload fails
        """
        try:
            # Create dataset if it doesn't exist
            self.create_dataset_if_not_exists(dataset_name)
            
            # Clean column names
            data.columns = [clean_column_name(col) for col in data.columns]
            logger.info("Cleaned column names:")
            logger.info(data.columns.tolist())
            
            table_id = f"{self.project_id}.{dataset_name}.{table_name}"
            
            # Delete table if it exists
            try:
                self.client.delete_table(table_id, not_found_ok=True)
                logger.info(f"Deleted existing table {table_id} if it existed")
            except Exception as e:
                logger.error(f"Error deleting table {table_id}: {str(e)}")
                raise UploadError(f"Failed to delete existing table: {str(e)}")
            
            # Create table with schema from DataFrame
            schema = []
            for col in data.columns:
                if col == 'processed_at':
                    schema.append(bigquery.SchemaField(col, 'DATETIME'))
                elif col in ['id', 'email', 'phone', 'partner', 'email_name_search_key', 'first_name', 'last_name']:
                    schema.append(bigquery.SchemaField(col, 'STRING'))
                elif col in ['utm_adid', 'utm_campaign', 'utm_medium', 'utm_source', 'digital_utm_campaign', 'digital_utm_medium', 'digital_utm_source', 'facebook_adid']:
                    schema.append(bigquery.SchemaField(col, 'STRING'))
                else:
                    schema.append(bigquery.SchemaField(col, 'STRING'))
            
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            logger.info(f"Created new table {table_id} with schema")
            
            # Convert data types to match schema
            data = convert_data_types(data, table.schema)
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
            
            # Upload the data
            job = self.client.load_table_from_dataframe(
                data,
                table_id,
                job_config=job_config
            )
            
            # Wait for the job to complete
            job.result()
            
            log_operation("data_upload", {
                "dataset": dataset_name,
                "table": table_name,
                "search_key": email_name_search_key,
                "num_rows": len(data)
            })
            
        except GoogleAPIError as e:
            log_error(e, {
                "dataset": dataset_name,
                "table": table_name,
                "search_key": email_name_search_key
            })
            raise UploadError(f"BigQuery API error: {str(e)}")
            
        except Exception as e:
            log_error(e, {
                "dataset": dataset_name,
                "table": table_name,
                "search_key": email_name_search_key
            })
            raise UploadError(f"Failed to upload data: {str(e)}")
            
    def get_table_schema(
        self,
        dataset_name: str,
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get the schema of a BigQuery table.
        
        Args:
            dataset_name: Name of the BigQuery dataset
            table_name: Name of the BigQuery table
            
        Returns:
            List of field definitions
            
        Raises:
            UploadError: If schema retrieval fails
        """
        try:
            table_id = f"{self.project_id}.{dataset_name}.{table_name}"
            table = self.client.get_table(table_id)
            return [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode
                }
                for field in table.schema
            ]
            
        except GoogleAPIError as e:
            log_error(e, {
                "dataset": dataset_name,
                "table": table_name
            })
            raise UploadError(f"Failed to get table schema: {str(e)}")
            
        except Exception as e:
            log_error(e, {
                "dataset": dataset_name,
                "table": table_name
            })
            raise UploadError(f"Unexpected error getting schema: {str(e)}") 