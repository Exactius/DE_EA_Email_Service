from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime
import hashlib

from .error_handling import TransformationError, log_error

def hash_value(value: str) -> str:
    """
    Hash a value using SHA-256.
    
    Args:
        value: Value to hash
        
    Returns:
        Hashed value as hex string
    """
    if pd.isna(value) or value == '':
        return None
    return hashlib.sha256(str(value).lower().strip().encode()).hexdigest()

def read_file() -> pd.DataFrame:
    """
    Read data from the CSV file.
    
    Returns:
        DataFrame containing the data
        
    Raises:
        TransformationError: If file reading fails
    """
    try:
        # Read CSV without specifying encoding
        file_path = "test_data/ContributionReport-21240998138.csv"
        print(f"Reading file from: {file_path}")
        df = pd.read_csv(file_path)
        print(f"File read successfully. Columns: {df.columns.tolist()}")
        return df
            
    except Exception as e:
        log_error(e, {"file": "ContributionReport-21240998138.csv"})
        raise TransformationError(f"Failed to read file: {str(e)}")

def check_file_structure() -> Dict[str, Any]:
    """
    Check the structure of the file.
    
    Returns:
        Dict containing file information:
        - columns: List of column names
        - num_rows: Number of rows
        - sample_data: First few rows of data
        - has_required_columns: Whether required columns are present
        
    Raises:
        TransformationError: If file reading fails
    """
    try:
        # Read the file
        df = read_file()
        
        # Get required columns from dtypes
        from .dtypes import REQUIRED_COLUMNS
        
        # Check structure
        result = {
            "columns": list(df.columns),
            "num_rows": len(df),
            "sample_data": df.head(3).to_dict('records'),
            "has_required_columns": all(col in df.columns for col in REQUIRED_COLUMNS),
            "missing_columns": [col for col in REQUIRED_COLUMNS if col not in df.columns]
        }
        
        return result
        
    except Exception as e:
        log_error(e, {"file": "ContributionReport-21240998138.csv"})
        raise TransformationError(f"Failed to check file structure: {str(e)}")

def process_test_data(
    partner: str,
    email_name_search_key: str,
    is_link: bool = False
) -> pd.DataFrame:
    """
    Process test data from the file.
    
    Args:
        partner: Partner identifier
        email_name_search_key: Email search key for filtering
        is_link: Whether to process as link data
        
    Returns:
        DataFrame containing processed data
        
    Raises:
        TransformationError: If processing fails
    """
    try:
        # Read the file
        df = read_file()
        print(f"Original columns: {df.columns.tolist()}")
        
        # Hash sensitive data
        if 'Personal Email' in df.columns:
            print("Hashing Personal Email column...")
            df['email'] = df['Personal Email'].apply(hash_value)
            df = df.drop('Personal Email', axis=1)
            print("Personal Email hashed and dropped")
            
        if 'Preferred Phone Number' in df.columns:
            print("Hashing Preferred Phone Number column...")
            df['phone'] = df['Preferred Phone Number'].apply(hash_value)
            df = df.drop('Preferred Phone Number', axis=1)
            print("Preferred Phone Number hashed and dropped")
            
        # Process Contact Name
        print("Checking for Contact Name column...")
        if 'Contact Name' in df.columns:
            print("Found Contact Name column, processing...")
            # Split name into parts and hash each
            name_parts = df['Contact Name'].str.split(' ', n=1, expand=True)
            print(f"Name parts shape: {name_parts.shape}")
            print(f"First few name parts:\n{name_parts.head()}")
            
            df['first_name'] = name_parts[0].apply(hash_value)
            df['last_name'] = name_parts[1].apply(hash_value)
            df = df.drop('Contact Name', axis=1)
            print("Contact Name split and hashed into first_name and last_name")
        else:
            print("Contact Name column not found in DataFrame")
            print(f"Available columns: {df.columns.tolist()}")
            
        # Rename UTM columns without hashing
        if 'utm_adid  (Exactius)' in df.columns:
            print("Renaming UTM Ad ID column...")
            df['utm_adid'] = df['utm_adid  (Exactius)']
            df = df.drop('utm_adid  (Exactius)', axis=1)
            print("UTM Ad ID renamed")
            
        if 'utm_campaign (Exactius)' in df.columns:
            print("Renaming UTM Campaign column...")
            df['utm_campaign'] = df['utm_campaign (Exactius)']
            df = df.drop('utm_campaign (Exactius)', axis=1)
            print("UTM Campaign renamed")
            
        if 'utm_medium (Exactius)' in df.columns:
            print("Renaming UTM Medium column...")
            df['utm_medium'] = df['utm_medium (Exactius)']
            df = df.drop('utm_medium (Exactius)', axis=1)
            print("UTM Medium renamed")
            
        if 'utm_source (Exactius)' in df.columns:
            print("Renaming UTM Source column...")
            df['utm_source'] = df['utm_source (Exactius)']
            df = df.drop('utm_source (Exactius)', axis=1)
            print("UTM Source renamed")
            
        if 'Digital Acquisition Data: UTM Campaign' in df.columns:
            print("Renaming Digital UTM Campaign column...")
            df['digital_utm_campaign'] = df['Digital Acquisition Data: UTM Campaign']
            df = df.drop('Digital Acquisition Data: UTM Campaign', axis=1)
            print("Digital UTM Campaign renamed")
            
        if 'Digital Acquisition Data: UTM Medium' in df.columns:
            print("Renaming Digital UTM Medium column...")
            df['digital_utm_medium'] = df['Digital Acquisition Data: UTM Medium']
            df = df.drop('Digital Acquisition Data: UTM Medium', axis=1)
            print("Digital UTM Medium renamed")
            
        if 'Digital Acquisition Data: UTM Source' in df.columns:
            print("Renaming Digital UTM Source column...")
            df['digital_utm_source'] = df['Digital Acquisition Data: UTM Source']
            df = df.drop('Digital Acquisition Data: UTM Source', axis=1)
            print("Digital UTM Source renamed")
            
        if 'uqaid - Facebook Ad ID (Exactius)' in df.columns:
            print("Renaming Facebook Ad ID column...")
            df['facebook_adid'] = df['uqaid - Facebook Ad ID (Exactius)']
            df = df.drop('uqaid - Facebook Ad ID (Exactius)', axis=1)
            print("Facebook Ad ID renamed")
        
        print(f"Columns after processing: {df.columns.tolist()}")
        
        # Add processing metadata
        df['processed_at'] = datetime.now()
        df['partner'] = partner
        df['email_name_search_key'] = email_name_search_key
        df['is_link'] = is_link
        
        return df
        
    except Exception as e:
        log_error(e, {
            "file": "ContributionReport-21240998138.csv",
            "partner": partner,
            "search_key": email_name_search_key,
            "is_link": is_link
        })
        raise TransformationError(f"Failed to process test data: {str(e)}") 