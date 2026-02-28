from typing import Dict, List, Optional
from datetime import datetime

# --- 1. The Models (How data looks) ---
class Broker:
    def __init__(self, id: str, name: str, allowed_formats: List[str]):
        self.id = id
        self.name = name
        self.allowed_formats = allowed_formats  # e.g., ["csv", "xlsx"]

class Corporate:
    def __init__(self, id: str, broker_id: str, name: str, webhook_url: str, insurer_format: str = "json"):
        self.id = id
        self.broker_id = broker_id  # Link to Broker
        self.name = name
        self.webhook_url = webhook_url  # Where to push their data
        self.insurer_format = insurer_format # "json" or "xml"

# --- 1. NEW MODEL: User ---
class User:
    def __init__(self, username: str, password: str, api_key: str):
        self.username = username
        self.password = password  # In production, hash this!
        self.api_key = api_key    # The key this user "owns"

class ApiKey:
    def __init__(self, key: str, corporate_id: str, is_active: bool = True):
        self.key = key
        self.corporate_id = corporate_id
        self.is_active = is_active

# --- 2. The Data (Our "Tables") ---
# In a real app, these would be SQL Tables.

BROKERS_DB = {
    "brk_marsh": Broker("brk_marsh", "Marsh Insurance", ["csv", "xlsx"]),
    "brk_aon": Broker("brk_aon", "Aon Brokers", ["csv"])  # Aon only allows CSV!
}

CORPORATES_DB = {
    "corp_infosys": Corporate(
        "corp_infosys", "brk_marsh", "Infosys Ltd",
        "https://webhook.site/c7940ad3-0303-4a07-ba71-d1b61c72e068",
        "json" # Infosys insurer wants JSON
    ),
    "corp_wipro": Corporate(
        "corp_wipro", "brk_aon", "Wipro Technologies",
        "https://webhook.site/c7940ad3-0303-4a07-ba71-d1b61c72e068",
        "xml"  # Wipro insurer wants XML
    )
}

# --- 2. NEW DATA: The HR Managers ---
USERS_DB = {
    "hr_infosys": User("hr@infosys.com", "admin123", "sk_live_infosys_001"),
    "hr_wipro":   User("hr@wipro.com", "admin123", "sk_live_wipro_999")
}

# The Keys (In production, these are hashed!)
API_KEYS_DB = {
    "sk_live_infosys_001": ApiKey("sk_live_infosys_001", "corp_infosys"),
    "sk_live_wipro_999": ApiKey("sk_live_wipro_999", "corp_wipro")
}

# --- 3. The Helper Functions (Our "Queries") ---

# --- 3. HELPER FUNCTIONS (The missing part) ---

def get_api_key_record(key: str) -> Optional[ApiKey]:
    """Retrieves an API Key object by its string value."""
    return API_KEYS_DB.get(key)

def get_corporate_record(corp_id: str) -> Optional[Corporate]:
    """Retrieves a Corporate object by its ID."""
    return CORPORATES_DB.get(corp_id)

def get_broker_record(broker_id: str) -> Optional[Broker]:
    """Retrieves a Broker object by its ID."""
    return BROKERS_DB.get(broker_id)

# --- 3. NEW HELPER ---
def authenticate_user(username: str, password: str) -> Optional[User]:
    """
    Simple check: Does user exist and password match?
    Returns the User object (which contains the API Key).
    """
    # In a real DB, you'd query WHERE username=...
    for user in USERS_DB.values():
        if user.username == username and user.password == password:
            return user
    return None