from app.core.outbound.base import BaseInsurerAdapter
from typing import Dict, Any


class HdfcErgoAdapter(BaseInsurerAdapter):
    def transform_addition(self, standard_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms standard payload into HDFC Ergo JSON format for Additions."""

        # HDFC uses camelCase and specific nested objects
        return {
            "requestType": "ENROLLMENT",
            "requestRefId": standard_payload.get("transaction_id"),
            "employee": {
                "empId": standard_payload.get("employee_code"),
                "personalInfo": {
                    "firstName": standard_payload.get("first_name"),
                    "lastName": standard_payload.get("last_name"),
                    "dateOfBirth": standard_payload.get("date_of_birth"),
                    "genderCode": "M" if standard_payload.get("gender") == "Male" else "F"
                },
                "coverage": {
                    "relation": standard_payload.get("relationship", "Self").upper(),
                    "baseSumInsured": standard_payload.get("sum_insured", 0.0),
                    "coverageStartDate": standard_payload.get("date_of_joining")
                }
            }
        }

    def transform_deletion(self, standard_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms standard payload into HDFC Ergo JSON format for Deletions."""

        return {
            "requestType": "CANCELLATION",
            "requestRefId": standard_payload.get("transaction_id"),
            "cancellationDetails": {
                "empId": standard_payload.get("employee_code"),
                "memberId": standard_payload.get("member_id"),
                "cancellationDate": standard_payload.get("date_of_leaving"),
                "reason": "EMPLOYEE_RESIGNED"
            }
        }

    def get_headers(self, api_key: str) -> Dict[str, str]:
        """HDFC uses standard Bearer token authentication."""
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }