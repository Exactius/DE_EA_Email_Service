from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import io
import base64
import hashlib
import logging

from ..utils import (
    TransformationError,
    log_operation,
    log_error,
    validate_dataframe
)

# Configure logging
logger = logging.getLogger(__name__)

def hash_value(value: str) -> str:
    """
    Hash a string value using SHA-256.
    
    Args:
        value: String value to hash
        
    Returns:
        Hashed value as hex string
    """
    if not isinstance(value, str):
        value = str(value)
    return hashlib.sha256(value.encode()).hexdigest()

class DataTransformationService:
    """Service for transforming data from email attachments"""
    
    def transform_data(
        self,
        attachment_ids: List[str],
        partner: str,
        email_name_search_key: str,
        attachment_data: str = None  # Add parameter for existing data
    ) -> pd.DataFrame:
        """
        Transform data from email attachments.
        
        Args:
            attachment_ids: List of attachment IDs to process
            partner: Partner identifier
            email_name_search_key: Email search key for filtering
            attachment_data: Optional pre-fetched attachment data
            
        Returns:
            DataFrame containing transformed data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            logger.info(f"Transforming data for partner {partner}")
            
            # Read CSV data
            logger.info("Reading CSV data")
            df = pd.read_csv(io.StringIO(attachment_data))
            logger.info(f"Initial DataFrame shape: {df.shape}")
            
            # Hash sensitive data
            logger.info("Processing sensitive data")
            if 'Personal Email' in df.columns:
                logger.info("Hashing Personal Email column")
                df['email'] = df['Personal Email'].apply(hash_value)
                df = df.drop('Personal Email', axis=1)
                
            if 'Preferred Phone Number' in df.columns:
                logger.info("Hashing Preferred Phone Number column")
                df['phone'] = df['Preferred Phone Number'].apply(hash_value)
                df = df.drop('Preferred Phone Number', axis=1)
                
            # Process Contact Name
            if 'Contact Name' in df.columns:
                logger.info("Processing Contact Name column")
                name_parts = df['Contact Name'].str.split(' ', n=1, expand=True)
                df['first_name'] = name_parts[0].apply(hash_value)
                df['last_name'] = name_parts[1].apply(hash_value)
                df = df.drop('Contact Name', axis=1)
                
            # Rename UTM columns without hashing
            utm_columns = {
                'utm_adid  (Exactius)': 'utm_adid',
                'utm_campaign (Exactius)': 'utm_campaign',
                'utm_medium (Exactius)': 'utm_medium',
                'utm_source (Exactius)': 'utm_source',
                'Digital Acquisition Data: UTM Campaign': 'digital_utm_campaign',
                'Digital Acquisition Data: UTM Medium': 'digital_utm_medium',
                'Digital Acquisition Data: UTM Source': 'digital_utm_source',
                'uqaid - Facebook Ad ID (Exactius)': 'facebook_adid',
                'Facebook Ad ID (Exactius)': 'facebook_adid'  # Adding alternative column name
            }
            
            for old_col, new_col in utm_columns.items():
                if old_col in df.columns:
                    logger.info(f"Renaming {old_col} to {new_col}")
                    df[new_col] = df[old_col]
                    df = df.drop(old_col, axis=1)
            
            # Add metadata
            logger.info("Adding metadata")
            df['processed_at'] = datetime.now()
            df['partner'] = partner
            df['email_name_search_key'] = email_name_search_key
            df['id'] = df.index.astype(str)
            
            # Ensure all required columns exist and are of correct type
            logger.info("Ensuring required columns")
            required_columns = ['id', 'email', 'phone', 'processed_at', 'partner', 'email_name_search_key']
            for col in required_columns:
                if col not in df.columns:
                    logger.info(f"Adding missing column: {col}")
                    df[col] = ''
                elif col in ['phone', 'email', 'id']:
                    logger.info(f"Converting {col} to string type")
                    df[col] = df[col].astype(str)
            
            logger.info(f"Final DataFrame shape: {df.shape}")
            
            # Validate the transformed data
            validate_dataframe(df)
            
            log_operation("data_transformation", {
                "partner": partner,
                "num_rows": len(df)
            })
            
            return df
            
        except Exception as e:
            log_error(e, {
                "partner": partner,
                "num_attachments": len(attachment_ids)
            })
            raise TransformationError(f"Failed to transform data: {str(e)}")
            
    def transform_link_data(
        self,
        attachment_ids: List[str],
        email_name_search_key: str
    ) -> pd.DataFrame:
        """
        Transform link data from email attachments.
        
        Args:
            attachment_ids: List of attachment IDs to process
            email_name_search_key: Email search key for filtering
            
        Returns:
            DataFrame containing transformed link data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            print(f"Transforming {len(attachment_ids)} link attachments")
            
            # Process each attachment
            dfs = []
            for attachment_id in attachment_ids:
                print(f"Processing attachment {attachment_id}")
                
                # Get attachment data
                attachment_data = self._get_attachment_data(attachment_id)
                
                # Read CSV data
                df = pd.read_csv(io.StringIO(attachment_data))
                print(f"Read CSV data with shape: {df.shape}")
                print(f"Columns: {df.columns.tolist()}")
                
                # Add metadata
                df['processed_at'] = datetime.now()
                df['email_name_search_key'] = email_name_search_key
                
                dfs.append(df)
                
            # Combine all DataFrames
            if not dfs:
                raise TransformationError("No data found in attachments")
                
            final_df = pd.concat(dfs, ignore_index=True)
            print(f"Combined DataFrame shape: {final_df.shape}")
            
            # Validate the transformed data
            validate_dataframe(final_df)
            
            log_operation("link_data_transformation", {
                "search_key": email_name_search_key,
                "num_rows": len(final_df)
            })
            
            return final_df
            
        except Exception as e:
            log_error(e, {
                "search_key": email_name_search_key,
                "num_attachments": len(attachment_ids)
            })
            raise TransformationError(f"Failed to transform link data: {str(e)}")
            
    def _get_attachment_data(self, attachment_id: str) -> str:
        """
        Get attachment data from Gmail API.
        
        Args:
            attachment_id: ID of the attachment to retrieve
            
        Returns:
            Decoded attachment data as string
            
        Raises:
            TransformationError: If retrieval fails
        """
        try:
            # Get the actual data from the email service
            from ..services.email_service import EmailService
            email_service = EmailService(None)  # We don't need auth for this
            
            # Get the message data
            message_data = email_service.api_call(
                endpoint="data",
                attachment_ids=[attachment_id]
            )
            
            if not message_data or not isinstance(message_data, tuple) or not message_data[0]:
                raise TransformationError("No message data found")
            
            # Extract the file content from the message
            file_content = message_data[0][0].get('data', '')
            if not file_content:
                raise TransformationError("No file content found in message")
            
            return file_content
            
        except Exception as e:
            raise TransformationError(f"Failed to get attachment data: {str(e)}") 