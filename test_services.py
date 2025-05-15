import logging
import sys
from src.services.auth_service import AuthService
from src.services.email_service import EmailService

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def test_email_attachments():
    try:
        # Initialize services
        project_id = "ex-colehaan"
        logger.info(f"Initializing services for project: {project_id}")
        
        # Test auth service
        logger.info("Testing auth service...")
        auth_service = AuthService(project_id)
        access_token = auth_service.get_access_token()
        logger.info(f"Got access token: {access_token[:10]}...")
        
        # Test email service
        logger.info("Testing email service...")
        email_service = EmailService(auth_service)
        
        # Test getting messages
        logger.info("Testing get messages...")
        messages = email_service.api_call(
            email_to="noreply@everyaction.com",
            endpoint="messages",
            access_token=access_token,
            email_name_search_key="exactius_contribution_report"
        )
        logger.info(f"Got {len(messages) if messages else 0} messages")
        
        if not messages:
            logger.info("No messages found")
            return
            
        # Get first message details
        logger.info("Getting first message details...")
        message_data = email_service.api_call(
            endpoint="data",
            access_token=access_token,
            attachment_ids=[messages[0]['id']]
        )
        logger.info(f"Message data: {message_data}")
        
        # Extract download link
        logger.info("Extracting download link...")
        download_url = email_service.extract_download_link(message_data)
        if download_url:
            logger.info(f"Found download URL: {download_url}")
            
            # Download file
            logger.info("Downloading file...")
            file_content = email_service.download_file(download_url)
            if file_content:
                logger.info(f"Successfully downloaded file of size: {len(file_content)} bytes")
            else:
                logger.error("Failed to download file")
        else:
            logger.error("No download link found in email")
        
        logger.info("Email attachment test completed!")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_email_attachments() 