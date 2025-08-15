"""
https://faculty180.interfolio.com/swagger/ui/hmac.html
Click: Download OpenAPI specification
Use openapi-generator to generate docs
Find a curl example - this python code generates the necessary header
and executes the curl command.
Result: Access denied.
"""
import base64
import hmac
import os
from datetime import UTC, datetime
from hashlib import sha1

from dotenv import load_dotenv

load_dotenv()

# Configuration
public_key = os.getenv("API_PUBLIC_KEY")
private_key = os.getenv("API_PRIVATE_KEY")
database_id = os.getenv("TENANT_1_DATABASE_ID")

# HTTP request details
request_verb = "GET"
# request_string = "/coursestaught?termid=termid_example&userlist=&data=data_example&q=q_example&limit=56&offset=56"
request_string = "/coursestaught?termid=2025Fall&limit=10&offset=0"
timestamp_string = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

# Create verb request string
verb_request_string = f"{request_verb}\n\n\n{timestamp_string}\n{request_string}"

# Generate HMAC-SHA1 hash
encrypted_string = hmac.new(
    private_key.encode(), verb_request_string.encode(), sha1
).digest()
signed_hash = base64.b64encode(encrypted_string).decode().rstrip("\n")

# Construct Authorization header
authorization_header = f"INTF {public_key}:{signed_hash}"

# Construct curl command
curl_command = [
    "curl", "-X", "GET",
    "-H", "Accept: application/json",
    "-H", f"TimeStamp: {timestamp_string}",
    "-H", f"Authorization: {authorization_header}",
    "-H", f"INTF-DatabaseID: {database_id}",
    f"https://faculty180.interfolio.com/api.php{request_string}"
]

# Execute curl command and capture output
try:
    result = subprocess.run(curl_command, capture_output=True, text=True, check=True)
    print(result.stdout)
except subprocess.CalledProcessError as e:
    print(f"Error executing curl command: {e.stderr}")
