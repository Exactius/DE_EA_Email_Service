from typing import Optional
import os
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import traceback
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from ..utils import (
    AuthError,
    log_operation,
    log_error,
    retry_with_backoff
)
from .secret_service import get_secret
import pickle

class AuthService:
    """Service for handling authentication with Google APIs"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.token_uri = 'https://oauth2.googleapis.com/token'
        self.SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
        self.credentials = None
        
        # Get credentials from Secret Manager
        try:
            print("Attempting to get credentials from Secret Manager...")
            creds = json.loads(get_secret())
            print("Successfully retrieved credentials")
            self.client_id = creds['client_id']
            self.client_secret = creds['client_secret']
            self.refresh_token = creds['refresh_token']
            self.token_uri = creds.get('token_url', self.token_uri)
        except Exception as e:
            print(f"Failed to load credentials: {str(e)}")
            raise AuthError(f"Failed to load credentials: {str(e)}")
            
    def get_credentials(self) -> Credentials:
        """Get Gmail API credentials."""
        try:
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    self.credentials = pickle.load(token)
                    
            if not self.credentials or not self.credentials.valid:
                if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                    self.credentials.refresh(Request())
                else:
                    # Get credentials from Secret Manager
                    secret_data = json.loads(get_secret())
                    flow = InstalledAppFlow.from_client_config(
                        secret_data,
                        self.SCOPES
                    )
                    self.credentials = flow.run_local_server(port=0)
                    
                with open('token.pickle', 'wb') as token:
                    pickle.dump(self.credentials, token)
                    
            return self.credentials
            
        except Exception as e:
            log_error("Failed to load credentials", error=e)
            raise AuthError("Failed to load credentials") from e
            
    def get_gmail_service(self):
        """Get Gmail API service."""
        try:
            credentials = self.get_credentials()
            return build('gmail', 'v1', credentials=credentials)
        except Exception as e:
            log_error("Failed to get Gmail service", error=e)
            raise AuthError("Failed to get Gmail service") from e
            
    @retry_with_backoff(max_retries=3)
    def get_access_token(self) -> str:
        """
        Get a valid access token using refresh token.
        
        Returns:
            str: Valid access token
            
        Raises:
            AuthError: If token refresh fails
        """
        try:
            print("\n=== Getting access token ===")
            print(f"Using refresh token: {self.refresh_token[:10]}...")
            print(f"Client ID: {self.client_id[:10]}...")
            print(f"Token URI: {self.token_uri}")
            
            credentials = Credentials(
                None,
                refresh_token=self.refresh_token,
                token_uri=self.token_uri,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            print("Credentials object created successfully")
            
            # Refresh the token if needed
            if not credentials.valid:
                print("Token is not valid, attempting to refresh...")
                try:
                    credentials.refresh(Request())
                    print("Token refreshed successfully")
                except Exception as e:
                    print(f"Error refreshing token: {str(e)}")
                    print(f"Error type: {type(e)}")
                    raise
            else:
                print("Token is still valid")
                
            log_operation("token_refresh", {
                "expires_in": credentials.expiry.timestamp() if credentials.expiry else None
            })
            
            print(f"Access token obtained: {credentials.token[:10]}...")
            return credentials.token
            
        except RefreshError as e:
            print(f"\nToken refresh error: {str(e)}")
            print(f"Error type: {type(e)}")
            log_error(e, {"token_uri": self.token_uri})
            raise AuthError(f"Failed to refresh token: {str(e)}")
            
        except Exception as e:
            print(f"\nUnexpected error during authentication: {str(e)}")
            print(f"Error type: {type(e)}")
            print(f"Error traceback: {traceback.format_exc()}")
            log_error(e, {"token_uri": self.token_uri})
            raise AuthError(f"Unexpected error during authentication: {str(e)}") 