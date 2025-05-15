from typing import Dict, Any, Tuple, Optional
import pandas as pd
from datetime import datetime
import traceback
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import asyncio
import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery

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

app = FastAPI()

# Partner configurations
PARTNER_CONFIGS = {
    'whitestork': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork"'
    },
    'exactius': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - exactius"'
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

@app.post("/process")
async def process_data(request: ProcessRequest):
    """
    Process data from an email attachment and upload to BigQuery.
    
    Args:
        request: ProcessRequest containing:
            - partner: Partner name
            - project_id: Google Cloud project ID
            - dataset: BigQuery dataset name
            - table_name: BigQuery table name
            - is_link: Whether to process as link data
            - email_id: Optional email ID to process
            - email_name_search_key: Optional email search key
        
    Returns:
        dict: Response with status and message
    """
    try:
        # Initialize services with project ID
        auth_service = AuthService(request.project_id)
        email_service = EmailService(auth_service)
        transformation_service = DataTransformationService()
        
        # Set email search key based on partner if not provided
        if not request.email_name_search_key:
            request.email_name_search_key = f'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - {request.partner}"'
        
        # Get access token
        access_token = auth_service.get_access_token()
        
        # Process email attachments
        attachment_data = await email_service.process_attachments(
            email_to="data.analysis@exacti.us",
            partner=request.partner,
            email_name_search_key=request.email_name_search_key,
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
            email_name_search_key=request.email_name_search_key,
            attachment_data=attachment_data.get("data")
        )
        
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
        
        # Delete the email after successful upload
        await email_service.delete_email(
            email_name_search_key=request.email_name_search_key,
            email_to="data.analysis@exacti.us",
            access_token=access_token
        )
        
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

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