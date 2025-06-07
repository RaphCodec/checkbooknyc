from typing import Dict, List, Optional, Union

import requests
from loguru import logger
from ._base import BaseClient, Criteria


class Revenue(BaseClient):
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        super().__init__(session, base_url)
        self.data_type = "Revenue"

    def fetch(
        self,
        records_from: Optional[int],
        max_records: Optional[int],
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ) -> str:
        """
        Builds a string XML request for the revenue endpoint using supported filters.
        """

        field_type: Dict[str, str] = {
            "budget_fiscal_year": "value",
            "fiscal_year": "value",
            "agency_code": "value",
            "revenue_class": "value",
            "fund_class": "value",
            "funding_class": "value",
            "revenue_category": "value",
            "revenue_source": "value",
            "revenue_expense_category": "value",
            "adopted": "range",
            "modified": "range",
            "recognized": "range",
            "conditional_category": "value",
            "remaining": "range",
            "budget_type": "value",
            "budget_name": "value",
            "funding_source": "value",
            "responsibility_center": "value",
            "program": "value",
            "project": "value",
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

        xml_body = self._base_request(
            self.data_type, criteria, records_from, max_records, response_columns
        )
        return self._parse(self._post(xml_body).decode("utf-8"))

    def fetch_all_records(
        self,
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        records_from = 1
        max_records = 20_000
        while True:
            records = self.fetch(records_from, max_records, response_columns, **filters)

            yield records

            if not records or len(records) < 20_000:
                logger.info("All records fetched.")
                break

            records_from += 20_000
