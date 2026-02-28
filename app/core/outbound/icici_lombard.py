from app.core.outbound.base import BaseInsurerAdapter
from typing import Dict, Any
import xmltodict


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
            "Accept": "application/xml"
        }