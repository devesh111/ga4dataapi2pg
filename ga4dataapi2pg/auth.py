from google.oauth2 import service_account
import os

def get_credentials(scopes=None):
    cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if cred_path:
        scopes = scopes or ['https://www.googleapis.com/auth/analytics.readonly']
        creds = service_account.Credentials.from_service_account_file(cred_path, scopes=scopes)
        return creds
    return None
