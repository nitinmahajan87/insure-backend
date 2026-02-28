import httpx
import os
import xmltodict
from typing import Dict, Any

INSURER_API_KEY = os.getenv("INSURER_API_KEY", "secret-token")

class InsurerConnector:
    @staticmethod
    async def push_to_insurer(payload: Dict[str, Any], target_url: str, format_type: str = "json") -> Dict[str, Any]:
        """
        Sends the processed payload to the specific Insurance House API.
        """
        if not target_url or "YOUR-UNIQUE-ID" in target_url:
            return {"status": "skipped", "reason": "No valid API URL configured for this corporate"}

        headers = {
            "X-API-Key": INSURER_API_KEY
        }

        if format_type.lower() == "xml":
            headers["Content-Type"] = "application/xml"
            xml_payload = xmltodict.unparse({"InsurancePayload": payload}, pretty=True)
            request_kwargs = {"content": xml_payload}
        else:
            headers["Content-Type"] = "application/json"
            request_kwargs = {"json": payload}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    target_url,
                    headers=headers,
                    timeout=10.0,
                    **request_kwargs
                )
                response.raise_for_status()

                return {
                    "status": "success",
                    "insurer_response_code": response.status_code,
                    "format_sent": format_type,
                    "delivered_to": target_url
                }
            except httpx.HTTPStatusError as exc:
                print(f"❌ Insurer Rejected Request: {exc.response.status_code}")
                return {"status": "failed", "error": f"Insurer rejected: {exc.response.text}"}
            except Exception as exc:
                print(f"❌ Network Error: {exc}")
                return {"status": "failed", "error": str(exc)}

    @staticmethod
    def push_to_insurer_sync(payload: Dict[str, Any], target_url: str, format_type: str = "json") -> Dict[str, Any]:
        """
        SYNCHRONOUS: Specifically for Celery Worker.
        """
        import requests  # Make sure 'requests' is installed

        headers = {"X-API-Key": INSURER_API_KEY}

        if format_type.lower() == "xml":
            headers["Content-Type"] = "application/xml"
            data = xmltodict.unparse({"InsurancePayload": payload}, pretty=True)
        else:
            headers["Content-Type"] = "application/json"
            import json
            data = json.dumps(payload)

        try:
            response = requests.post(target_url, data=data, headers=headers, timeout=15)
            # This triggers an exception for 4xx or 5xx responses
            response.raise_for_status()

            return {
                "status": "success",
                "insurer_response_code": response.status_code,
                "data": response.json() if "application/json" in response.headers.get("Content-Type",
                                                                                      "") else response.text
            }
        except requests.exceptions.RequestException as e:
            # We catch it here and re-raise or handle so the Worker knows to retry
            print(f"❌ API Call Failed: {str(e)}")
            raise e