import logging
from typing import Dict, Any, Optional

import requests
import xmltodict

from app.core.outbound.base import BaseInsurerAdapter

logger = logging.getLogger(__name__)


class IciciLombardAdapter(BaseInsurerAdapter):
    def transform_addition(self, standard_payload: Dict[str, Any]) -> str:
        """Transforms standard payload into ICICI Lombard XML for Additions."""

        # ICICI typically expects a specific nested structure
        icici_dict = {
            "HealthInsuranceEnrollment": {
                "Transaction": {
                    "Type": "ADD",
                    "Timestamp": standard_payload.get("timestamp")
                },
                "MemberDetails": {
                    "MemberCode": standard_payload.get("employee_code"),
                    "FirstName": standard_payload.get("first_name"),
                    "LastName": standard_payload.get("last_name"),
                    "DOB": standard_payload.get("date_of_birth"),
                    "Gender": standard_payload.get("gender"),
                    "Relation": standard_payload.get("relationship", "Self")
                },
                "PolicyDetails": {
                    "SumInsured": standard_payload.get("sum_insured", 0.0),
                    "JoiningDate": standard_payload.get("date_of_joining")
                }
            }
        }
        # Convert dictionary to an XML string
        return xmltodict.unparse(icici_dict, pretty=True)

    def transform_deletion(self, standard_payload: Dict[str, Any]) -> str:
        """Transforms standard payload into ICICI Lombard XML for Deletions."""

        icici_dict = {
            "HealthInsuranceEnrollment": {
                "Transaction": {
                    "Type": "DELETE",
                    "Timestamp": standard_payload.get("timestamp")
                },
                "MemberDetails": {
                    "MemberCode": standard_payload.get("employee_code"),
                    # ICICI might require member_id if it was generated during addition
                    "InsurerMemberID": standard_payload.get("member_id", "")
                },
                "PolicyDetails": {
                    "ExitDate": standard_payload.get("date_of_leaving")
                }
            }
        }
        return xmltodict.unparse(icici_dict, pretty=True)

    def get_headers(self, api_key: str) -> Dict[str, str]:
        """ICICI requires XML content type and a custom auth header."""
        return {
            "X-ICICI-AuthToken": api_key,
            "Content-Type": "application/xml",
            "Accept": "application/xml",
        }

    # ICICI Lombard status values → our normalised vocabulary
    _STATUS_MAP = {
        "ISSUED":   "APPROVED",
        "ACTIVE":   "APPROVED",
        "REJECTED": "REJECTED",
        "DECLINED": "REJECTED",
        "PENDING":  "PENDING",
        "INPROGRESS": "PENDING",
    }

    def check_policy_status(
        self, transaction_id: str, api_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Polls ICICI Lombard's status endpoint for a given transaction.
        ICICI uses a POST with an XML body and returns XML.

        ICICI Lombard XML response shape (representative):
          <PolicyStatusResponse>
            <TransactionRef>...</TransactionRef>
            <PolicyStatus>ISSUED | REJECTED | PENDING</PolicyStatus>
            <PolicyId>IL-2024-XXXXX</PolicyId>
            <PolicyNumber>IL0099887</PolicyNumber>
            <EffectiveDate>2024-04-01</EffectiveDate>
            <RejectionReason></RejectionReason>
          </PolicyStatusResponse>
        """
        request_xml = xmltodict.unparse({
            "PolicyStatusRequest": {
                "TransactionRef": transaction_id,
            }
        }, pretty=True)

        try:
            resp = requests.post(
                "https://api.icicilombard.com/group-health/policy/status",
                data=request_xml,
                headers=self.get_headers(api_key),
                timeout=10,
            )
            resp.raise_for_status()

            parsed = xmltodict.parse(resp.text)
            body = parsed.get("PolicyStatusResponse", {})

            raw_status = body.get("PolicyStatus", "PENDING")
            normalised = self._STATUS_MAP.get(raw_status.upper(), "PENDING")

            return {
                "status":                normalised,
                "insurer_reference_id":  body.get("PolicyId"),
                "policy_number":         body.get("PolicyNumber"),
                "policy_effective_date": body.get("EffectiveDate"),
                "rejection_reason":      body.get("RejectionReason") or None,
            }
        except requests.exceptions.RequestException as exc:
            logger.warning(
                f"ICICI Lombard status poll failed for tx '{transaction_id}': {exc}"
            )
            return None