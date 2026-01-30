"""
Script to generate a new Gmail OAuth refresh token.
"""

import json
from google_auth_oauthlib.flow import InstalledAppFlow

# Gmail read-only scope
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

CLIENT_CONFIG = {
    "installed": {
        "client_id": "610305024408-50l8k0lkcsdvg975cb68p24epfs4nu51.apps.googleusercontent.com",
        "client_secret": "d0Xr_nd6JG1CdD8bb-yqjLw0",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"]
    }
}

def main():
    print("Starting OAuth flow...")
    print("A browser window will open. Login with shai@exacti.us")
    print()

    flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)

    # Try port 9090 instead
    credentials = flow.run_local_server(port=9090, open_browser=True)

    # Build the new secret JSON
    new_secret = {
        "client_id": CLIENT_CONFIG["installed"]["client_id"],
        "client_secret": CLIENT_CONFIG["installed"]["client_secret"],
        "refresh_token": credentials.refresh_token,
        "token_url": "https://oauth2.googleapis.com/token"
    }

    print("\n" + "="*60)
    print("SUCCESS! New credentials generated.")
    print("="*60)
    print("\nNew secret JSON (copy this):")
    print(json.dumps(new_secret, indent=2))
    print("\n" + "="*60)
    print("\nTo update the secret, run:")
    print("gcloud secrets versions add gmail_secret --data-file=- --project=815522637671")
    print("Then paste the JSON above and press Ctrl+D")
    print("="*60)

if __name__ == "__main__":
    main()
