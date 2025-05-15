import logging
import sys
from src.services.auth_service import AuthService
from src.services.email_service import EmailService
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def test_email_service():
    try:
        # Load environment variables
        load_dotenv()
        
        # Initialize services
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        auth_service = AuthService(project_id)
        email_service = EmailService(auth_service)
        
        # Get access token
        access_token = auth_service.get_access_token()
        
        # Test parameters
        email_to = "noreply@everyaction.com"
        partner = "test_partner"
        email_name_search_key = "test_search_key"
        
        # Process attachments
        result = email_service.process_attachments(
            email_to=email_to,
            partner=partner,
            email_name_search_key=email_name_search_key,
            access_token=access_token,
            is_link=True
        )
        
        print("\n=== Test Results ===")
        print(f"Status: {result.get('status')}")
        print(f"Message: {result.get('message')}")
        if result.get('data'):
            print("Data received successfully")
        print("===================\n")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_email_service() 