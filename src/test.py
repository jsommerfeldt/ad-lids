import requests
from urllib.parse import quote
import msal, json, base64

TENANT_ID = "549e5d19-024b-4b10-ac2e-58d96a47fe41"
CLIENT_ID = "eac8976a-19da-4bf7-ba45-0b483ad23e4f"
CLIENT_SECRET = "" # omitted from prompt for security
SCOPE = ["https://graph.microsoft.com/.default"]

app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)
result = app.acquire_token_for_client(scopes=SCOPE)
token = result["access_token"]
print(token)

payload = token.split(".")[1] + "=="
claims = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
print(claims.get("roles"))

