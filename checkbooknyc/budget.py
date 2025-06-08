from typing import Dict, List, Optional, Union
import difflib

import requests
from loguru import logger
from ._base import BaseClient, Criteria


class Budget(BaseClient):
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
        params: Optional[Dict[str, Union[str, int, float]]] = None,
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

        if params:
            invalid_keys = list(filter(lambda k: k not in field_type, params.keys()))
            if invalid_keys:
                closest_matches = {
                    key: difflib.get_close_matches(word=key, possibilities=field_type.keys(), n=3, cutoff=0.2)
                    for key in invalid_keys
                }
                raise ValueError(
                    f"Invalid parameters: {invalid_keys}. Closest potential matches: {closest_matches}"
                )
            criteria.extend(
                map(
                    lambda item: {
                        "name": item[0],
                        "type": field_type[item[0]],
                        "value": str(item[1]),
                    },
                    params.items(),
                )
            )

        xml_body = self._base_request(
            self.data_type, criteria, records_from, max_records, response_columns
        )
        return self._parse(self._post(xml_body).decode("utf-8"))

    def fetch_all(
        self,
        response_columns: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[str, int, float]]] = None,
    ):
        records_from = 1
        max_records = 20_000
        while True:
            records = self.fetch(records_from, max_records, response_columns, params)

            yield records

            if not records or len(records) < 20_000:
                logger.info("All records fetched.")
                break

            records_from += 20_000
