import logging
from typing import Dict, Any, Optional

import requests

from app.core.outbound.base import BaseInsurerAdapter

logger = logging.getLogger(__name__)


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

    # HDFC Ergo status values → our normalised vocabulary
    _STATUS_MAP = {
        "ENROLLED": "APPROVED",
        "ACTIVE":   "APPROVED",
        "REJECTED": "REJECTED",
        "DECLINED": "REJECTED",
        "PENDING":  "PENDING",
    }

    def check_policy_status(
        self, transaction_id: str, api_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Polls HDFC Ergo's status endpoint using the Idempotency-Key we sent
        them as the reference ID (requestRefId).

        HDFC Ergo JSON response shape (representative):
          {
            "requestRefId": "...",
            "policyStatus": "ENROLLED" | "REJECTED" | "PENDING",
            "policyId":     "HDFC-POL-2024-XXXXX",
            "policyNumber": "P0012345",
            "startDate":    "2024-04-01",
            "rejectionReason": null | "OVER_AGE_LIMIT"
          }
        """
        try:
            resp = requests.get(
                f"https://api.hdfcergo.com/policy/status/{transaction_id}",
                headers=self.get_headers(api_key),
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            raw_status = data.get("policyStatus", "PENDING")
            normalised = self._STATUS_MAP.get(raw_status.upper(), "PENDING")

            return {
                "status":               normalised,
                "insurer_reference_id": data.get("policyId"),
                "policy_number":        data.get("policyNumber"),
                "policy_effective_date": data.get("startDate"),
                "rejection_reason":     data.get("rejectionReason"),
            }
        except requests.exceptions.RequestException as exc:
            logger.warning(
                f"HDFC Ergo status poll failed for tx '{transaction_id}': {exc}"
            )
            return None