from typing import Dict, Any, Tuple, Optional
import pandas as pd
from datetime import datetime
import traceback
from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery
import io
import base64
import hashlib

from ..utils import (
    TransformationError,
    UploadError,
    log_operation,
    log_error,
    AuthError,
    EmailProcessingError
)
from ..services.bigquery_service import BigQueryService
from ..services.data_transformation_service import DataTransformationService
from ..services.email_service import EmailService
from ..services.auth_service import AuthService
from ..config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create test_data directory if it doesn't exist
os.makedirs("test_data", exist_ok=True)

router = APIRouter()

# Health check endpoint
@router.get("/health")
async def health_check():
    return {"status": "healthy"}

# Partner configurations
PARTNER_CONFIGS = {
    'whitestrok': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - whitestrok"'
    },
    'exactius': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - exactius"'
    },
    'styt': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Styt_Contribution_Report"'
    }
    # Add more partners as needed
}

class ProcessRequest(BaseModel):
    partner: str
    project_id: str
    dataset: str
    table_name: str
    is_link: bool = False
    email_id: Optional[str] = None
    email_name_search_key: Optional[str] = None
    csv_data: Optional[str] = None  # Base64 encoded CSV data
    source_type: str = "email"  # "email" or "csv"
    csv_filename: Optional[str] = None  # Name of the CSV file to process

@router.post("/process")
async def process_data(request: ProcessRequest):
    """
    Process data from either email attachment or direct CSV upload and upload to BigQuery.
    
    Args:
        request: ProcessRequest containing:
            - partner: Partner name
            - project_id: Google Cloud project ID
            - dataset: BigQuery dataset name
            - table_name: BigQuery table name
            - is_link: Whether to process as link data
            - email_id: Optional email ID to process
            - email_name_search_key: Optional email search key
            - csv_data: Optional base64 encoded CSV data
            - source_type: "email" or "csv" to specify data source
            - csv_filename: Name of the CSV file to process (for csv source_type)
        
    Returns:
        dict: Response with status and message
    """
    try:
        # Initialize services
        auth_service = AuthService(request.project_id)
        email_service = EmailService(auth_service)
        transformation_service = DataTransformationService()
        
        # Get email search key from PARTNER_CONFIGS if using email source
        if request.source_type == "email":
            if request.partner not in PARTNER_CONFIGS:
                raise HTTPException(status_code=400, detail=f"Unknown partner: {request.partner}")
                
            # Use the exact search key from PARTNER_CONFIGS
            email_name_search_key = PARTNER_CONFIGS[request.partner]['email_name_search_key']
            print(f"Using configured search key: {email_name_search_key}")
            
            # Get access token
            access_token = auth_service.get_access_token()
            
            # Process email attachments
            attachment_data = await email_service.process_attachments(
                email_to="data.analysis@exacti.us",
                partner=request.partner,
                email_name_search_key=email_name_search_key,
                access_token=access_token
            )
            
            if attachment_data.get("status") == "error":
                raise HTTPException(status_code=400, detail=attachment_data.get("message"))
                
            if not attachment_data.get("data"):
                raise HTTPException(status_code=404, detail="No data found in attachment")
            
            # Process the data
            df = transformation_service.transform_data(
                attachment_ids=attachment_data.get("message_ids", []),
                partner=request.partner,
                email_name_search_key=email_name_search_key,
                attachment_data=attachment_data.get("data")
            )
            
            # Initialize BigQuery service and upload data
            try:
                bq_service = BigQueryService(request.project_id)
                bq_service.upload_data(
                    dataset_name=request.dataset,
                    table_name=request.table_name,
                    data=df,
                    email_name_search_key=email_name_search_key
                )
                logger.info(f"Successfully uploaded {len(df)} rows to BigQuery")
            except Exception as e:
                logger.error(f"Failed to upload to BigQuery: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Failed to upload to BigQuery: {str(e)}")
            
            # Delete the email after successful processing
            await email_service.delete_email(
                email_name_search_key=email_name_search_key,
                email_to="data.analysis@exacti.us",
                access_token=access_token
            )
            
        elif request.source_type == "csv":
            if not request.csv_filename:
                raise HTTPException(status_code=400, detail="No CSV filename provided")
                
            # Construct the path to the CSV file
            csv_path = os.path.join("test_data", request.csv_filename)
            if not os.path.exists(csv_path):
                raise HTTPException(status_code=404, detail=f"CSV file not found: {request.csv_filename}")
            
            # Read the CSV file
            try:
                # First read the file to get the actual header
                with open(csv_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # Skip SEP= line if it exists
                    start_idx = 1 if lines[0].strip().startswith('SEP=') else 0
                    header = lines[start_idx].strip()
                    actual_columns = header.split(',')
                    print("\nActual columns from CSV:")
                    print(actual_columns)
                
                # Now read the CSV with the actual columns
                df = pd.read_csv(
                    csv_path,
                    encoding='utf-8',
                    on_bad_lines='skip',
                    low_memory=False,
                    skiprows=start_idx + 1,  # Skip header row and SEP= line if it exists
                    quoting=1,  # QUOTE_ALL
                    dtype=str,  # Read all columns as strings initially
                    names=actual_columns  # Use actual columns from CSV
                )
                
                # Print the columns we actually got
                print("\nColumns from DataFrame:")
                print(df.columns.tolist())
                
                # Print first few rows to verify data alignment
                print("\nFirst few rows of raw data:")
                print(df.head().to_string())
                
                # Create mapping from actual columns to expected columns
                column_mapping = {
                    'Contribution ID': 'contribution_id',
                    'VANID': 'vanid',
                    'last_name': 'last_name',
                    'first_name': 'first_name',
                    'Date Received': 'date_received',
                    'Amount': 'amount',
                    'Source Code': 'source_code',
                    'Designation': 'designation',
                    'Payment Method': 'payment_method',
                    'Remaining Amount': 'remaining_amount',
                    'Financial Batch': 'financial_batch',
                    'utm_medium (Exactius)': 'utm_medium',
                    'utm_source (Exactius)': 'utm_source',
                    'Card Type': 'card_type',
                    'Covered Costs': 'covered_costs',
                    'Covered Costs Amount': 'covered_costs_amount',
                    'First Contribution Date': 'first_contribution_date',
                    'Form ID': 'form_id',
                    'Form Name': 'form_name',
                    'Is Recurring Commitment': 'is_recurring_commitment',
                    'Mailing City': 'mailing_city',
                    'Mailing Country': 'mailing_country',
                    'Mailing State': 'mailing_state',
                    'Online Reference Number': 'online_reference_number',
                    'Personal Email': 'email',
                    'Preferred Phone Number': 'phone',
                    'Status': 'status',
                    'Total Number of Contributions': 'total_number_of_contributions',
                    'Digital Acquisition Data: UTM Campaign': 'digital_utm_campaign',
                    'Digital Acquisition Data: UTM Medium': 'digital_utm_medium',
                    'Digital Acquisition Data: UTM Source': 'digital_utm_source',
                    'utm_adid  (Exactius)': 'utm_adid',
                    'utm_campaign (Exactius)': 'utm_campaign',
                    'uqaid  to  Facebook Ad ID (Exactius)': 'facebook_adid'
                }
                
                # Rename columns using the mapping
                df = df.rename(columns=column_mapping)
                
                # Hash sensitive fields
                def hash_value(value):
                    if pd.isna(value) or value == '':
                        return ''
                    return hashlib.sha256(str(value).strip().lower().encode()).hexdigest()
                
                # Hash sensitive fields
                sensitive_fields = ['email', 'phone', 'last_name', 'first_name']
                for field in sensitive_fields:
                    if field in df.columns:
                        df[field] = df[field].apply(hash_value)
                        print(f"\nHashed {field}")
                        print(f"Sample values for {field}:")
                        print(df[field].head())
                
                # Convert dates to MM/DD/YYYY format
                date_columns = ['date_received', 'first_contribution_date']
                for date_col in date_columns:
                    if date_col in df.columns:
                        try:
                            # First handle numeric dates (Excel dates)
                            def convert_excel_date(x):
                                try:
                                    # Check if the value is numeric
                                    if pd.notna(x) and str(x).replace('.', '').isdigit():
                                        # Convert Excel date number to datetime
                                        return pd.to_datetime('1899-12-30') + pd.Timedelta(days=float(x))
                                    return x
                                except:
                                    return x
                            
                            # Convert Excel dates first
                            df[date_col] = df[date_col].apply(convert_excel_date)
                            
                            # Then convert to datetime
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                            
                            # Finally format to MM/DD/YYYY
                            df[date_col] = df[date_col].dt.strftime('%m/%d/%Y')
                            
                            print(f"\nConverted {date_col} to MM/DD/YYYY format")
                            print(f"Sample values for {date_col}:")
                            print(df[date_col].head())
                            
                            # Check for NaN values
                            nan_count = df[date_col].isna().sum()
                            print(f"\nNumber of NaN values in {date_col}: {nan_count}")
                            if nan_count > 0:
                                print("\nSample rows with NaN values:")
                                print(df[df[date_col].isna()][['contribution_id', date_col]].head())
                                
                        except Exception as e:
                            print(f"Warning: Could not convert {date_col}: {str(e)}")
                            # Keep original values if conversion fails
                            pass
                
                print("\nAfter hashing sensitive fields and date conversion (Top 5 rows):")
                print("==========================================")
                print(df.head().to_string())
                
                # Reorder columns to match BigQuery schema
                column_order = [
                    'contribution_id', 'vanid', 'date_received', 'amount', 'source_code', 'designation',
                    'payment_method', 'remaining_amount', 'financial_batch', 'card_type', 'covered_costs',
                    'covered_costs_amount', 'first_contribution_date', 'form_id', 'form_name',
                    'is_recurring_commitment', 'mailing_city', 'mailing_country', 'mailing_state',
                    'online_reference_number', 'email', 'phone', 'status', 'total_number_of_contributions',
                    'digital_utm_campaign', 'digital_utm_medium', 'digital_utm_source', 'utm_adid',
                    'utm_campaign', 'utm_medium', 'utm_source', 'facebook_adid', 'last_name', 'first_name'
                ]
                
                # Ensure all columns exist
                for col in column_order:
                    if col not in df.columns:
                        print(f"Warning: Column {col} not found in DataFrame")
                        df[col] = None
                
                # Reorder columns
                df = df[column_order]
                print("\nFinal Data (Top 5 rows):")
                print("=====================")
                print(df.head().to_string())
                print("\nFinal Columns:")
                print(df.columns.tolist())
                
                # Ensure all columns are strings except processed_at
                for col in df.columns:
                    if col != 'processed_at':
                        df[col] = df[col].astype(str)
                
                # Add required columns if they don't exist
                df['processed_at'] = pd.Timestamp.now()  # Use pandas Timestamp for datetime
                df['partner'] = request.partner
                df['email_name_search_key'] = f"direct_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                df['id'] = df['contribution_id']  # Already a string from above
                
                # Print detailed DataFrame information
                print("\nDataFrame Information:")
                print("=====================")
                print(f"Shape: {df.shape}")
                print("\nColumns:")
                for col in df.columns:
                    print(f"- {col}")
                print("\nFirst few rows:")
                print(df.head())
                print("\nData types:")
                print(df.dtypes)
                
                # Check for empty or problematic columns
                empty_cols = df.columns[df.isna().all()].tolist()
                if empty_cols:
                    print("\nEmpty columns found:", empty_cols)
                
                # Check for columns with all zeros or empty strings
                zero_cols = []
                for col in df.columns:
                    if df[col].dtype in ['int64', 'float64']:
                        if (df[col] == 0).all():
                            zero_cols.append(col)
                    elif df[col].dtype == 'object':
                        if (df[col] == '').all():
                            zero_cols.append(col)
                if zero_cols:
                    print("\nColumns with all zeros or empty strings:", zero_cols)
                
                # Convert DataFrame to CSV string for transformation
                csv_data = df.to_csv(index=False, quoting=1)  # QUOTE_ALL
                
                # For direct CSV uploads, we don't need to use the transformation service
                # Just ensure the data types are correct
                df['processed_at'] = pd.Timestamp.now()  # Use pandas Timestamp for datetime
                df['partner'] = request.partner
                df['email_name_search_key'] = f"direct_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                df['id'] = df['contribution_id']  # Already a string from above
                
                # Print detailed DataFrame information
                print("\nDataFrame Information:")
                print("=====================")
                print(f"Shape: {df.shape}")
                print("\nColumns:")
                for col in df.columns:
                    print(f"- {col}")
                print("\nFirst few rows:")
                print(df.head())
                print("\nData types:")
                print(df.dtypes)
                
                # Check for empty or problematic columns
                empty_cols = df.columns[df.isna().all()].tolist()
                if empty_cols:
                    print("\nEmpty columns found:", empty_cols)
                
                # Check for columns with all zeros or empty strings
                zero_cols = []
                for col in df.columns:
                    if df[col].dtype in ['int64', 'float64']:
                        if (df[col] == 0).all():
                            zero_cols.append(col)
                    elif df[col].dtype == 'object':
                        if (df[col] == '').all():
                            zero_cols.append(col)
                if zero_cols:
                    print("\nColumns with all zeros or empty strings:", zero_cols)
                
                # Clean column names for BigQuery
                df.columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('.', '_') for col in df.columns]
                
                # Upload to BigQuery
                client = bigquery.Client(project=request.project_id)
                dataset_id = f"{request.project_id}.{request.dataset}"
                table_id = f"{dataset_id}.{request.table_name}"
                
                # Create dataset if it doesn't exist
                try:
                    client.get_dataset(dataset_id)
                except Exception:
                    dataset = bigquery.Dataset(dataset_id)
                    dataset.location = "US"
                    client.create_dataset(dataset, exists_ok=True)
                
                # Delete table if it exists
                try:
                    client.delete_table(table_id)
                    print(f"Deleted existing table {table_id}")
                except Exception as e:
                    print(f"Table {table_id} does not exist or could not be deleted: {str(e)}")
                
                # Upload the data
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                
                job = client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()
                
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error reading CSV file: {str(e)}")
            
        else:
            raise HTTPException(status_code=400, detail=f"Invalid source_type: {request.source_type}")
        
        return {
            "status": "success",
            "message": f"Successfully processed and uploaded {len(df)} rows"
        }
        
    except (UploadError, TransformationError, AuthError, EmailProcessingError) as e:
        log_error("Error processing data", {"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log_error("Unexpected error", {"error": str(e)})
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

@router.post("/process-csv")
async def process_csv_file(
    file: UploadFile = File(...),
    partner: str = None,
    project_id: str = None,
    dataset: str = None,
    table_name: str = None,
    is_link: bool = False
):
    """
    Process a CSV file directly and upload to BigQuery.
    
    Args:
        file: CSV file to process
        partner: Partner name
        project_id: Google Cloud project ID
        dataset: BigQuery dataset name
        table_name: BigQuery table name
        is_link: Whether to process as link data
        
    Returns:
        dict: Response with status and message
    """
    try:
        if not all([partner, project_id, dataset, table_name]):
            raise HTTPException(status_code=400, detail="Missing required parameters")
            
        # Initialize services
        transformation_service = DataTransformationService()
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Transform the data
        df = transformation_service.transform_data(
            attachment_ids=[],  # No attachment IDs for direct upload
            partner=partner,
            email_name_search_key=f"direct_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            attachment_data=df.to_csv(index=False)  # Convert DataFrame to CSV string
        )
        
        # Clean column names for BigQuery
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('.', '_') for col in df.columns]
        
        # Upload to BigQuery
        client = bigquery.Client(project=project_id)
        dataset_id = f"{project_id}.{dataset}"
        table_id = f"{dataset_id}.{table_name}"
        
        # Create dataset if it doesn't exist
        try:
            client.get_dataset(dataset_id)
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            client.create_dataset(dataset, exists_ok=True)
        
        # Delete table if it exists
        try:
            client.delete_table(table_id)
            print(f"Deleted existing table {table_id}")
        except Exception as e:
            print(f"Table {table_id} does not exist or could not be deleted: {str(e)}")
        
        # Upload the data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        return {
            "status": "success",
            "message": f"Successfully processed and uploaded {len(df)} rows"
        }
        
    except Exception as e:
        log_error("Error processing CSV file", {"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(router, host="0.0.0.0", port=8000)

def process_email_attachments(
    project_id: str,
    dataset_name: str,
    table_name: str,
    partner: str,
    email_name_search_key: str,
    email_to: str,
    is_link: bool = False
) -> Tuple[bool, str]:
    """
    Process data from email attachments and upload to BigQuery.
    
    Args:
        project_id: Google Cloud project ID
        dataset_name: BigQuery dataset name
        table_name: BigQuery table name
        partner: Partner identifier
        email_name_search_key: Email search key for filtering
        email_to: Email address to process
        is_link: Whether to process as link data
        
    Returns:
        Tuple of (success, message)
    """
    try:
        print(f"Starting process_email_attachments with params: project_id={project_id}, dataset={dataset_name}, table={table_name}, partner={partner}, search_key={email_name_search_key}, email={email_to}, is_link={is_link}")
        
        # Log start of processing
        log_operation("process_start", {
            "project": project_id,
            "dataset": dataset_name,
            "table": table_name,
            "partner": partner,
            "search_key": email_name_search_key,
            "email_to": email_to,
            "is_link": is_link
        })
        
        # Initialize services
        print("Initializing services...")
        try:
            bq_service = BigQueryService(project_id)
            transform_service = DataTransformationService()
            auth_service = AuthService(project_id)
            email_service = EmailService(auth_service)
            print("Services initialized successfully")
        except Exception as e:
            print(f"Failed to initialize services: {str(e)}")
            raise
            
        # Get access token
        print("Getting access token...")
        try:
            access_token = auth_service.get_access_token()
            print("Access token obtained successfully")
            print(f"Token: {access_token[:10]}...")  # Print first 10 chars for verification
        except Exception as e:
            print(f"Failed to get access token: {str(e)}")
            raise
            
        # Process email attachments
        print("Processing email attachments...")
        try:
            attachment_data = email_service.process_attachments(
                email_to=email_to,
                partner=partner,
                email_name_search_key=email_name_search_key,
                access_token=access_token
            )
            
            if attachment_data.get("status") == "error":
                logger.error(f"Failed to process attachments: {attachment_data.get('message')}")
                return False, attachment_data.get("message")
            
            if not attachment_data.get("data"):
                print("No data found in attachment")
                return False, "No data found in attachment"
            
            # Transform data
            print("Transforming data...")
            try:
                if is_link:
                    df = transform_service.transform_link_data(
                        attachment_ids=attachment_data.get("message_ids", []),
                        email_name_search_key=email_name_search_key
                    )
                else:
                    df = transform_service.transform_data(
                        attachment_ids=attachment_data.get("message_ids", []),
                        partner=partner,
                        email_name_search_key=email_name_search_key,
                        attachment_data=attachment_data.get("data")
                    )
                print(f"Data transformed successfully, shape: {df.shape}")
            except Exception as e:
                print(f"Failed to transform data: {str(e)}")
                raise
            
            # Clean column names for BigQuery
            df.columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('.', '_') for col in df.columns]
            
            # Upload to BigQuery
            print("Uploading to BigQuery...")
            try:
                bq_service.upload_data(
                    dataset_name=dataset_name,
                    table_name=table_name,
                    data=df,
                    email_name_search_key=email_name_search_key
                )
                print("Upload complete")
            except Exception as e:
                print(f"Failed to upload to BigQuery: {str(e)}")
                raise
            
            # Log success
            log_operation("process_complete", {
                "project": project_id,
                "dataset": dataset_name,
                "table": table_name,
                "partner": partner,
                "search_key": email_name_search_key,
                "email_to": email_to,
                "num_rows": len(df)
            })
            
            return True, "Data processed and uploaded successfully"
            
        except Exception as e:
            print(f"Failed to process attachments: {str(e)}")
            raise
        
    except TransformationError as e:
        error_msg = f"Failed to process data: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        log_error(e, {
            "project": project_id,
            "dataset": dataset_name,
            "table": table_name,
            "partner": partner,
            "search_key": email_name_search_key,
            "email_to": email_to
        })
        return False, error_msg
        
    except UploadError as e:
        error_msg = f"Failed to upload data: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        log_error(e, {
            "project": project_id,
            "dataset": dataset_name,
            "table": table_name,
            "partner": partner,
            "search_key": email_name_search_key,
            "email_to": email_to
        })
        return False, error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        log_error(e, {
            "project": project_id,
            "dataset": dataset_name,
            "table": table_name,
            "partner": partner,
            "search_key": email_name_search_key,
            "email_to": email_to
        })
        return False, error_msg 