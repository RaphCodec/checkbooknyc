from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from xml.etree import ElementTree as ET

import requests
from loguru import logger
from .client import CheckbookNYC, Criteria

class Budget(CheckbookNYC):
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        super().__init__(session, base_url)
        self.data_type = "Budget"

    def fetch(
        self,
        records_from: Optional[int],
        max_records: Optional[int],
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ) -> str:
        """
        Builds a string XML request for the budget endpoint using supported filters.
        """

        field_type: Dict[str, str] = {
            "year": "value",
            "budget_code": "value",
            "budget_code_name": "value",
            "agency_code": "value",
            "department_code": "value",
            "expense_category": "value",
            "adopted": "range",
            "modified": "range",
            "pre_encumbered": "range",
            "encumbered": "range",
            "accrued_expense": "range",
            "cash_expense": "range",
            "post_adjustment": "range",
            "conditional_category": "value",
            "budget_type": "value",
            "budget_name": "value",
            "funding_source": "value",
            "responsibility_center": "value",
            "program": "value",
            "project": "value",
            "committed": "range",
            "actual_amount": "range",
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

        xml_body = self._base_request(self.data_type, criteria, records_from, max_records, response_columns)
        return self._parse(self._post(xml_body).decode("utf-8"))


    def fetch_all(
        self,
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        records_from = 1
        max_records = 20_000
        while True:
            records = self.fetch(
                records_from, max_records, response_columns, **filters
            )

            yield records

            if not records or len(records) < 20_000:
                logger.info("All records fetched.")
                break

            records_from += 20_000
