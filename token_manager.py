"""
Token Manager for Collibra CDQ API authentication in Databricks.
Handles token generation, caching, and automatic refresh on expiry.
"""
import requests
import json
from datetime import datetime, timedelta


class CollibraTokenManager:
    """
    Manages authentication tokens for Collibra CDQ API.
    
    Features:
    - Caches tokens in memory during pipeline execution
    - Automatically refreshes expired tokens
    - Provides 5-minute buffer before expiry to prevent mid-step failures
    - Supports force refresh on API 401 errors
    """
    
    def __init__(self, base_url: str, username: str, password: str, region: str = "apac"):
        """
        Initialize token manager.
        
        Args:
            base_url: Collibra CDQ base URL (e.g., https://jnj-apac-comm-dq.collibra.jnj.com)
            username: Collibra username
            password: Collibra password
            region: Region identifier for logging (apac or cn)
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.region = region.upper()
        
        self.token = None
        self.token_expiry = None
        self.verify_ssl = False  # For internal J&J SSL
        self.token_url = f"{self.base_url}/v2/auth/signin"
    
    def get_token(self, force_refresh: bool = False) -> str:
        """
        Get valid token, refreshing if expired.
        
        Args:
            force_refresh: If True, skip expiry check and fetch new token immediately
            
        Returns:
            Valid authentication token string
            
        Raises:
            Exception: If token retrieval fails
        """
        if force_refresh or self._is_token_expired():
            self._refresh_token()
        return self.token
    
    def _is_token_expired(self) -> bool:
        """
        Check if token is expired or about to expire.
        Uses 5-minute buffer to prevent failures mid-step.
        
        Returns:
            True if token is missing, expired, or expiring soon
        """
        if not self.token or not self.token_expiry:
            return True
        
        buffer = timedelta(minutes=5)
        return datetime.now() >= (self.token_expiry - buffer)
    
    def _refresh_token(self) -> None:
        """
        Fetch fresh token from Collibra API.
        
        Raises:
            Exception: If sign-in request fails
        """
        payload = {
            "username": self.username,
            "password": self.password,
            "iss": "public"
        }
        
        try:
            print(f"[{self.region}] Refreshing token from {self.token_url}...")
            
            response = requests.post(
                self.token_url,
                json=payload,
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"[{self.region}] Failed! Status Code: {response.status_code}")
                print(f"[{self.region}] Response: {response.text[:500]}")
                response.raise_for_status()
            
            data = response.json()
            self.token = data.get("token")
            
            if not self.token:
                raise ValueError(f"Token not found in response: {data}")
            
            # Assume 1-hour expiry; adjust if API provides expiry time
            self.token_expiry = datetime.now() + timedelta(hours=1)
            
            username_from_response = data.get("username", "unknown")
            print(f"[{self.region}] ✓ Token refreshed for user: {username_from_response}")
            print(f"[{self.region}]   Expires at: {self.token_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except requests.exceptions.RequestException as e:
            print(f"[{self.region}] Request failed: {e}")
            raise
        except Exception as e:
            print(f"[{self.region}] Unexpected error: {e}")
            raise
    
    def get_auth_header(self) -> dict:
        """
        Get Authorization header dict ready for requests.
        
        Returns:
            Dict with Authorization header
        """
        token = self.get_token()
        return {"Authorization": f"Bearer {token}"}
