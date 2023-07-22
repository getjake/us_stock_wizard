import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from us_stock_wizard import StockRootDirectory

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "googleauth.json"


class GoogleDriveUtils:
    """
    Uploading files to Google Drive

    Example:
        gdrive = GoogleDriveUtils()
        gdrive.upload("/path/to/file.xlsx", "demo_table.xlsx")
    """

    def __init__(self) -> None:
        env = StockRootDirectory.env()
        self.folder_id = env.get("GDRIVE_PARENT_FOLDER_ID")
        if not self.folder_id:
            raise Exception("Please set GDRIVE_PARENT_FOLDER_ID in .env file.")
        self.cred_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), SERVICE_ACCOUNT_FILE
        )
        if not os.path.exists(self.cred_file):
            raise Exception(
                f"Please create {SERVICE_ACCOUNT_FILE} file in this directory."
            )

    def authenticate(self):
        creds = service_account.Credentials.from_service_account_file(
            self.cred_file, scopes=SCOPES
        )
        return creds

    def upload(self, file_path: str, name: str) -> bool:
        try:
            creds = self.authenticate()
            service = build("drive", "v3", credentials=creds)

            file_metadata = {
                "name": name,
                "parents": [self.folder_id],
            }

            service.files().create(body=file_metadata, media_body=file_path).execute()

            return True
        except Exception as e:
            print(e)
            return False
