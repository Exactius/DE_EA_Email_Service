from google.cloud import secretmanager
from src.utils import log_error

def get_secret() -> str:
    """
    Get credentials from Google Secret Manager.
    
    Returns:
        The secret value as a string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/815522637671/secrets/gmail_secret/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        log_error("Failed to load credentials", {"error": str(e)})
        raise 