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

# Column mapping for Recurring Commitment Report (WhiteStork sustaining donors)
RECURRING_COMMITMENT_COLUMNS = {
    'Recurring Commitment ID': 'recurring_commitment_id',
    'VANID': 'vanid',
    'Start Date': 'start_date',
    'End Date': 'end_date',
    'Amount': 'amount',
    'Currency': 'currency',
    'Frequency': 'frequency',
    'Total Amount Received to Date': 'total_received',
    'Total Amount Expected to Date': 'total_expected',
    'Status': 'status',
    'Payment Method': 'payment_method',
    'Designation': 'designation',
    'Financial Household ID': 'financial_household_id',
    'Source Code': 'source_code'
}

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

def normalize_data(df: pd.DataFrame, email_name_search_key: str) -> pd.DataFrame:
    """
    Normalize data by handling different column names and creating standardized columns.
    
    Args:
        df: Input DataFrame
        email_name_search_key: Email search key for filtering
        
    Returns:
        Normalized DataFrame
    """
    try:
        logger.info("Normalizing data")
        logger.info(f"Available columns: {df.columns.tolist()}")
        
        # Create a copy to avoid modifying the original
        normalized_df = df.copy()
        
        # Handle different phone column names
        phone_columns = [
            'preferred_phone',
            'Preferred Phone Number',
            'Phone Number',
            'Phone',
            'Mobile',
            'Mobile Number',
            'Cell Phone',
            'Cell'
        ]
        
        # Find which phone column exists
        phone_col = None
        for col in phone_columns:
            if col in normalized_df.columns:
                phone_col = col
                logger.info(f"Found phone column: {phone_col}")
                break
        
        # Create fb_phone column safely
        if phone_col:
            normalized_df["fb_phone"] = normalized_df[phone_col]
            logger.info("Created fb_phone column from existing phone column")
        else:
            # If no phone column found, create empty fb_phone column
            normalized_df["fb_phone"] = ""
            logger.warning("No phone column found, created empty fb_phone column")
        
        # Handle different email column names
        email_columns = [
            'preferred_email',
            'Preferred Email',
            'Personal Email',
            'Email',
            'Email Address'
        ]
        
        # Find which email column exists
        email_col = None
        for col in email_columns:
            if col in normalized_df.columns:
                email_col = col
                logger.info(f"Found email column: {email_col}")
                break
        
        # Create standardized email column
        if email_col:
            normalized_df["email"] = normalized_df[email_col]
            logger.info("Created standardized email column")
        else:
            # If no email column found, create empty email column
            normalized_df["email"] = ""
            logger.warning("No email column found, created empty email column")
        
        # Add metadata
        normalized_df['email_name_search_key'] = email_name_search_key
        normalized_df['processed_at'] = datetime.now()
        
        logger.info(f"Normalized DataFrame shape: {normalized_df.shape}")
        return normalized_df
        
    except Exception as e:
        logger.error(f"Error in normalize_data: {str(e)}")
        raise TransformationError(f"Failed to normalize data: {str(e)}")

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
            logger.info(f"Initial columns: {df.columns.tolist()}")
            
            # Normalize data first to handle different column names
            df = normalize_data(df, email_name_search_key)
            
            # Hash sensitive data
            logger.info("Processing sensitive data")
            if 'Personal Email' in df.columns:
                logger.info("Hashing Personal Email column")
                df['email'] = df['Personal Email'].apply(hash_value)
                df = df.drop('Personal Email', axis=1)
            elif 'Preferred Email' in df.columns:
                logger.info("Hashing Preferred Email column")
                df['email'] = df['Preferred Email'].apply(hash_value)
                df = df.drop('Preferred Email', axis=1)

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
                'Facebook Ad ID (Exactius)': 'facebook_adid',  # Adding alternative column name
                'Mailing Zip/Postal': 'mailing_zip',
                'Recurring Commitment ID': 'recurring_commitment_id'  # New field for recurring donations
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

    def transform_recurring_commitment_data(
        self,
        attachment_ids: List[str],
        partner: str,
        email_name_search_key: str,
        attachment_data: str = None
    ) -> pd.DataFrame:
        """
        Transform Recurring Commitment Report data from EveryAction.

        This is for WhiteStork sustaining donor metrics.
        NO HASHING - just column renaming and metadata.
        Contact Name is dropped (not needed).

        Args:
            attachment_ids: List of attachment IDs to process
            partner: Partner identifier (e.g., 'whitestork_recurring')
            email_name_search_key: Email search key for filtering
            attachment_data: Pre-fetched CSV data as string

        Returns:
            DataFrame containing transformed recurring commitment data

        Raises:
            TransformationError: If transformation fails
        """
        try:
            logger.info(f"Transforming recurring commitment data for partner {partner}")

            # Read CSV data
            logger.info("Reading CSV data")
            df = pd.read_csv(io.StringIO(attachment_data))
            logger.info(f"Initial DataFrame shape: {df.shape}")
            logger.info(f"Initial columns: {df.columns.tolist()}")

            # Drop Contact Name column (no PII needed for this report)
            if 'Contact Name' in df.columns:
                logger.info("Dropping Contact Name column (not needed)")
                df = df.drop('Contact Name', axis=1)

            # Rename columns using the mapping
            logger.info("Renaming columns using RECURRING_COMMITMENT_COLUMNS mapping")
            renamed_count = 0
            for old_col, new_col in RECURRING_COMMITMENT_COLUMNS.items():
                if old_col in df.columns:
                    logger.info(f"Renaming: '{old_col}' -> '{new_col}'")
                    df = df.rename(columns={old_col: new_col})
                    renamed_count += 1
            logger.info(f"Renamed {renamed_count} columns")

            # Clean amount fields - remove $ and convert to float
            amount_columns = ['amount', 'total_received', 'total_expected']
            for col in amount_columns:
                if col in df.columns:
                    # Remove $ sign and commas, convert to float
                    df[col] = df[col].astype(str).str.replace('$', '', regex=False)
                    df[col] = df[col].str.replace(',', '', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                    logger.info(f"Cleaned amount column: {col}")

            # Convert date columns to proper format
            date_columns = ['start_date', 'end_date']
            for col in date_columns:
                if col in df.columns:
                    # Handle empty/null end dates
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
                    # Replace 'NaT' string with None
                    df[col] = df[col].replace('NaT', None)
                    logger.info(f"Converted date column: {col}")

            # Add metadata
            logger.info("Adding metadata")
            df['processed_at'] = datetime.now()
            df['partner'] = partner
            df['email_name_search_key'] = email_name_search_key

            # Use recurring_commitment_id as the primary key if available
            if 'recurring_commitment_id' in df.columns:
                df['id'] = df['recurring_commitment_id'].astype(str)
            else:
                df['id'] = df.index.astype(str)

            # Ensure string columns are properly typed
            string_columns = ['vanid', 'currency', 'frequency', 'status',
                            'payment_method', 'designation', 'financial_household_id', 'source_code']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    # Replace 'nan' strings with empty string
                    df[col] = df[col].replace('nan', '')

            # Clean column names for BigQuery - replace spaces and special chars with underscores
            df.columns = [
                col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('.', '_')
                for col in df.columns
            ]
            logger.info("Cleaned column names for BigQuery compatibility")

            logger.info(f"Final DataFrame shape: {df.shape}")
            logger.info(f"Final columns: {df.columns.tolist()}")

            log_operation("recurring_commitment_transformation", {
                "partner": partner,
                "num_rows": len(df)
            })

            return df

        except Exception as e:
            log_error(e, {
                "partner": partner,
                "num_attachments": len(attachment_ids)
            })
            raise TransformationError(f"Failed to transform recurring commitment data: {str(e)}")

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
                
                # Normalize data to handle different column names
                df = normalize_data(df, email_name_search_key)
                
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