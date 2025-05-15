from typing import Dict, Any, Optional
import logging
import functools
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessingError(Exception):
    """Base exception for data processing errors"""
    pass

class AuthError(DataProcessingError):
    """Exception for authentication errors"""
    pass

class EmailProcessingError(DataProcessingError):
    """Exception for email processing errors"""
    pass

class TransformationError(DataProcessingError):
    """Exception for data transformation errors"""
    pass

class UploadError(DataProcessingError):
    """Exception for data upload errors"""
    pass

class ValidationError(DataProcessingError):
    """Exception for data validation errors"""
    pass

def log_operation(operation: str, details: Dict[str, Any]) -> None:
    """
    Log an operation with details.
    
    Args:
        operation: Name of the operation
        details: Dictionary of operation details
    """
    logger.info(f"Operation: {operation}", extra={
        "operation": operation,
        "details": details,
        "timestamp": datetime.now().isoformat()
    })

def log_error(error: Exception, context: Dict[str, Any]) -> None:
    """
    Log an error with context.
    
    Args:
        error: The exception that occurred
        context: Dictionary of context information
    """
    logger.error(
        f"Error: {str(error)}",
        exc_info=True,
        extra={
            "error_type": error.__class__.__name__,
            "error_message": str(error),
            "context": context,
            "timestamp": datetime.now().isoformat()
        }
    )

def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0):
    """
    Decorator for retrying operations with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        log_error(e, {
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "delay": delay
                        })
                        time.sleep(delay)
                        delay *= 2
                    else:
                        raise last_exception
                        
        return wrapper
    return decorator 