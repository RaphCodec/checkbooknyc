from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from xml.etree import ElementTree as ET

import requests
from loguru import logger
from .client import CheckbookNYC, Criteria


class Spending(CheckbookNYC):
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        super().__init__(session, base_url)
        self.data_type = "Spending"

    def build_spending_request(
        self,
        records_from: Optional[int],
        max_records: Optional[int],
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ) -> str:
        """
        Builds a string XML request for the spending endpoint using supported filters.
        """

        field_type: Dict[str, str] = {
            "fiscal_year": "value",
            "payee_name": "value",
            "payee_code": "value",
            "vendor_code": "value",
            "document_id": "value",
            "agency_code": "value",
            "issue_date": "range",
            "department_code": "value",
            "check_amount": "range",
            "expense_category": "value",
            "contract_id": "value",
            "capital_project_code": "value",
            "spending_category": "value",
            "budget_name": "value",
            "commodity_line": "value",
            "entity_contract_number": "value",
            "other_government_entities_code": "value",
            "mwbe_category": "value",
            "industry": "value",
            "funding_source": "value",
            "responsibility_center": "value",
            "purchase_order_type": "value",
            "amount_spent": "value",
            "conditional_category": "value",
        }

        criteria: List[Criteria] = []

        for key, value in filters.items():
            if key not in field_type.keys():
                logger.warning(f"Key: {key} is not valid and will be ignored.")
                continue
            criteria.append(
                {
                    "name": key,
                    "type": field_type[key],
                    "value": str(value),
                }
            )

        xml = self._base_request(self.data_type, criteria, records_from, max_records, response_columns)
        return xml

    def fetch(
        self,
        records_from: Optional[int] = None,
        max_records: Optional[int] = None,
        get_all_records: bool = False,
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        """
        Sends a POST request to the spending endpoint with given filter criteria.
        """
        if get_all_records:
            return self._fetch_all_records(response_columns, **filters)
        else:
            xml_body = self.build_spending_request(
                records_from, max_records, response_columns, **filters
            )
            return self._parse(self._post(xml_body).decode("utf-8"))

    def _fetch_all_records(
        self,
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        records_from = 1
        max_records = 20_000
        while True:
            xml_body = self.build_spending_request(
                records_from, max_records, response_columns, **filters
            )
            records = self._parse(self._post(xml_body).decode("utf-8"))
            yield records

            if not records or len(records) < 20_000:
                logger.info("No more records to fetch")
                break

            records_from += 20_000
