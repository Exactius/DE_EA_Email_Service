from typing import List, Optional, Dict, Any, Final
import base64
import io
import pandas as pd
import httpx
import asyncio
import json
import logging
import sys
from datetime import datetime
import re
import html
import os
from bs4 import BeautifulSoup

from ..utils import (
    EmailProcessingError,
    log_operation,
    log_error,
    retry_with_backoff
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

# Set httpx logging to WARNING to prevent request/response logging
logging.getLogger("httpx").setLevel(logging.WARNING)

def create_session(access_token: str, limits: httpx.Limits) -> tuple[httpx.Client, Dict[str, str]]:
    """Create HTTP session with headers"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    session = httpx.Client(limits=limits)
    return session, headers

async def async_prepare_attachment_urls(base_url: str, list_of_ids: List[str]) -> List[str]:
    """Prepare URLs for attachment requests"""
    return [
        f"{base_url}/users/me/messages/{msg_id}/attachments/{att_id}"
        for msg_id, att_id in list_of_ids
    ]

async def async_prepare_data_urls(base_url: str, list_of_ids: List[str]) -> List[str]:
    """Prepare URLs for message data requests"""
    return [
        f"{base_url}/users/me/messages/{msg_id}"
        for msg_id in list_of_ids
    ]

async def async_prepare_emails_to_delete(
    base_url: str,
    list_of_ids: List[str],
    is_new_data: bool,
    is_link: bool
) -> List[str]:
    """Prepare URLs for email deletion requests"""
    return [
        f"{base_url}/users/me/messages/{msg_id}/trash"
        for msg_id in list_of_ids
    ]

async def async_run(
    urls: List[str],
    headers: Dict[str, str],
    message_type: str = None,
    partner: str = None,
    email_name_search_key: str = None
) -> List[Dict[str, Any]]:
    """Execute async requests"""
    async with httpx.AsyncClient() as client:
        tasks = [client.get(url, headers=headers) for url in urls]
        responses = await asyncio.gather(*tasks)
        
        results = []
        for response in responses:
            if response.status_code == 200:
                data = response.json()
                results.append(data)
            else:
                logger.error(f"Request failed: {response.status_code} - {response.text}")
                
        return results

def make_request(
    session: httpx.Client,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any] = None,
    timeout: int = 60,
    payload: Dict[str, Any] = None,
    method: str = "get"
) -> httpx.Response:
    """Make HTTP request"""
    try:
        if method.lower() == "get":
            response = session.get(url, headers=headers, params=params, timeout=timeout)
        elif method.lower() == "post":
            response = session.post(url, headers=headers, json=payload, timeout=timeout)
        else:
            raise ValueError(f"Unsupported method: {method}")
            
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        raise

def get_message(response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract messages from response data"""
    messages = response_data.get('messages', [])
    return messages

class EmailService:
    """Service for handling email operations"""
    
    def __init__(self, auth_service):
        logger.info("=== Initializing EmailService ===")
        self.auth_service = auth_service
        self.base_url = "https://gmail.googleapis.com/gmail/v1"  # Gmail API endpoint
        self.user_id = "data.analysis@exacti.us"  # The Gmail account to use
        logger.info("EmailService initialized")
        
    @retry_with_backoff(max_retries=3)
    async def api_call(
        self,
        email_to: str = None,
        endpoint: str = None,
        access_token: str = None,
        timeout: int = 60,
        list_of_ids: Optional[List[str]] = None,
        attachment_ids: Optional[List[str]] = None,
        payload: Dict[str, Any] = None,
        method: str = "get",
        message_type: str = None,
        partner: str = None,
        email_name_search_key: str = None,
        is_new_data: bool = False,
        is_link: bool = False,
    ):
        try:
            limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
            session, headers = create_session(access_token, limits)

            if endpoint == "attachments":
                attachments = await async_prepare_attachment_urls(
                    base_url=self.base_url,
                    list_of_ids=list_of_ids
                )
                return await async_run(
                    urls=attachments,
                    headers=headers,
                    message_type=message_type,
                    partner=partner,
                    email_name_search_key=email_name_search_key,
                )
            elif endpoint == "data":
                data = await async_prepare_data_urls(
                    base_url=self.base_url,
                    list_of_ids=attachment_ids,
                )
                if data[0] is None:
                    return data
                return (
                    await async_run(
                        urls=data,
                        headers=headers,
                        message_type=message_type,
                        partner=partner,
                        email_name_search_key=email_name_search_key,
                    ),
                    data,
                )
            elif endpoint == "delete":
                deleted_emails = await async_prepare_emails_to_delete(
                    base_url=self.base_url,
                    list_of_ids=list_of_ids,
                    is_new_data=is_new_data,
                    is_link=is_link,
                )
                return await async_run(
                    urls=deleted_emails,
                    headers=headers,
                    message_type=message_type,
                    partner=partner,
                    email_name_search_key=email_name_search_key,
                )
            elif endpoint == "messages":
                # Use email_name_search_key for Gmail API search
                query = email_name_search_key  # The search key now includes subject: prefix
                print(f"Using search query: {query}")
                # Add order by date descending to get latest messages first
                params = {
                    "q": query,
                    "orderBy": "date",
                    "sortOrder": "descending",
                    "maxResults": 10  # Get more messages to ensure we find the right one
                }
                response = make_request(
                    session=session,
                    url=f"{self.base_url}/users/{self.user_id}/messages",
                    headers=headers,
                    params=params,
                    timeout=timeout,
                    payload=payload,
                    method=method,
                )
                if 200 <= response.status_code < 300:
                    logger.info("Success! Messages have been retrieved")
                    res = response.json()
                    return get_message(res)
                else:
                    logger.error(f"Failed to get messages: {response.status_code} - {response.text}")
                    return []
            else:
                logger.error(f"Error! Status code: {response.status_code}")

        except Exception as error:
            logger.error(f"API call failed: {str(error)}")
            raise EmailProcessingError(f"API call failed: {str(error)}")

    def extract_download_link(self, message_data: Dict[str, Any]) -> Optional[str]:
        """Extract download link from email body using BeautifulSoup."""
        try:
            if not message_data or not isinstance(message_data, tuple) or not message_data[0]:
                logger.error("No message data or message_data[0] is empty.")
                return None
            message = message_data[0][0]  # Extract the first message from the list
            if 'payload' not in message:
                logger.error("No 'payload' in message.")
                return None
            payload = message['payload']
            body_data = None

            # Try top-level body
            if 'body' in payload and 'data' in payload['body'] and payload['body']['data']:
                body_data = payload['body']['data']
                logger.info("Found body data in top-level body")
            # Try parts
            elif 'parts' in payload:
                logger.info(f"Checking {len(payload['parts'])} parts for body data")
                for part in payload['parts']:
                    if part.get('mimeType') == 'text/html' and 'data' in part.get('body', {}):
                        body_data = part['body']['data']
                        logger.info("Found body data in parts")
                        break

            if not body_data:
                logger.error("No body data found in message!")
                return None

            # Decode base64
            body_data = body_data.replace('-', '+').replace('_', '/')
            padding = len(body_data) % 4
            if padding:
                body_data += '=' * (4 - padding)
            decoded_html = base64.b64decode(body_data).decode('utf-8')
            logger.info("Successfully decoded HTML body")

            # Parse HTML and extract the link
            soup = BeautifulSoup(decoded_html, 'html.parser')
            link = soup.find('a', string=lambda text: text and 'Click here' in text)
            if link and link.has_attr('href'):
                url = link['href']
                logger.info(f"Found download URL: {url}")
                return url
            else:
                logger.error("No 'Click here' link found in email body. Available links:")
                for a in soup.find_all('a'):
                    logger.error(f"Link text: {a.text}, href: {a.get('href')}")
                return None

        except Exception as e:
            logger.error(f"Failed to extract download link: {str(e)}")
            return None

    def download_file(self, url: str) -> Optional[bytes]:
        """Download file from EveryAction"""
        try:
            logger.info(f"Attempting to download file from URL: {url}")
            response = httpx.get(url, timeout=60)
            if response.status_code == 200:
                logger.info("File downloaded successfully")
                return response.content
            else:
                logger.error(f"Failed to download file. Status code: {response.status_code}, Response: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to download file: {str(e)}")
            return None

    async def process_attachments(
        self,
        email_to: str,
        partner: str,
        email_name_search_key: str,
        access_token: str,
        is_link: bool = False
    ) -> Dict[str, Any]:
        """
        Process email attachments or download links
        """
        try:
            # Get messages
            logger.info(f"Searching for messages with key: {email_name_search_key}")
            messages = await self.api_call(
                email_to=email_to,
                endpoint="messages",
                access_token=access_token,
                email_name_search_key=email_name_search_key
            )
            
            if not messages:
                logger.error("No messages found matching the search criteria")
                return {"status": "success", "message": "No new messages found"}
            
            logger.info(f"Found {len(messages)} messages")
            
            # Initialize data dictionary
            data = {
                "status": "success",
                "message": "Processing attachments",
                "data": "",
                "message_ids": []
            }
            
            # Get first message details
            logger.info(f"Getting details for message ID: {messages[0]['id']}")
            message_data = await self.api_call(
                endpoint="data",
                access_token=access_token,
                attachment_ids=[messages[0]['id']]
            )
            
            # Extract download link
            download_url = self.extract_download_link(message_data)
            if download_url:
                # Download the file
                file_content = self.download_file(download_url)
                if file_content:
                    # Auto-detect encoding (UTF-16 or UTF-8)
                    try:
                        decoded_content = file_content.decode('utf-8')
                        logger.info("File decoded as UTF-8")
                    except UnicodeDecodeError:
                        try:
                            decoded_content = file_content.decode('utf-16')
                            logger.info("File decoded as UTF-16")
                        except UnicodeDecodeError:
                            # Last resort: try latin-1 (never fails but may produce garbage)
                            decoded_content = file_content.decode('latin-1')
                            logger.warning("File decoded as latin-1 (fallback)")

                    data["data"] = decoded_content
                    data["message_ids"].append(messages[0]['id'])
                    logger.info("File downloaded and decoded successfully")
                else:
                    logger.error("Failed to download file content")
                    return {"status": "error", "message": "Failed to download file content"}
            else:
                logger.error("No download link found in message")
                return {"status": "error", "message": "No download link found in message"}
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to process attachments: {str(e)}")
            raise EmailProcessingError(f"Failed to process attachments: {str(e)}")

    async def delete_email(self, email_name_search_key: str, email_to: str, access_token: str) -> None:
        """
        Delete the email after successful processing.
        
        Args:
            email_name_search_key: The search key to find the email
            email_to: The email address to search in
            access_token: The access token for Gmail API
        """
        try:
            # Get the message ID
            messages = await self.api_call(
                email_to=email_to,
                endpoint="messages",
                access_token=access_token,
                email_name_search_key=email_name_search_key
            )
            
            if messages:
                for message in messages:
                    # Move message to trash using modify endpoint
                    url = f"{self.base_url}/users/{self.user_id}/messages/{message['id']}/modify"
                    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
                    session, headers = create_session(access_token, limits)
                    
                    # Send POST request to move to trash
                    response = make_request(
                        session=session,
                        url=url,
                        headers=headers,
                        method="post",
                        payload={"removeLabelIds": ["INBOX"], "addLabelIds": ["TRASH"]}
                    )
                    
                    if response.status_code == 200:
                        logger.info(f"Moved email with ID: {message['id']} to trash")
                    else:
                        logger.error(f"Failed to move email to trash: {response.status_code} - {response.text}")
            else:
                logger.warning("No emails found to delete")
                
        except Exception as e:
            logger.error(f"Error deleting email: {str(e)}")
            raise 