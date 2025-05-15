import pandas as pd
from typing import Optional
from .error_handling import ValidationError

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate a DataFrame for required columns and data types.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(df, pd.DataFrame):
        raise ValidationError("Input must be a pandas DataFrame")
        
    if df.empty:
        raise ValidationError("DataFrame is empty")
        
    # Check for required columns
    required_columns = ['id', 'email', 'phone', 'processed_at', 'partner', 'email_name_search_key']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValidationError(f"Missing required columns: {', '.join(missing_columns)}")
        
    # Check data types
    if not df['id'].dtype == 'object':
        raise ValidationError("'id' column must be string type")
    if not df['email'].dtype == 'object':
        raise ValidationError("'email' column must be string type")
    if not df['phone'].dtype == 'object':
        raise ValidationError("'phone' column must be string type")
    if not pd.api.types.is_datetime64_any_dtype(df['processed_at']):
        raise ValidationError("'processed_at' column must be datetime type")
    if not df['partner'].dtype == 'object':
        raise ValidationError("'partner' column must be string type")
    if not df['email_name_search_key'].dtype == 'object':
        raise ValidationError("'email_name_search_key' column must be string type") 