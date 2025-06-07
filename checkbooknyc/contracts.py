from typing import Dict, List, Literal, Optional, Union
import difflib

import requests
from loguru import logger
from ._base import BaseClient, Criteria


class Contracts(BaseClient):
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        super().__init__(session, base_url)
        self.data_type = "Contracts"

    def fetch(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        records_from: Optional[int] = None,
        max_records: Optional[int] = None,
        response_columns: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[str, int, float]]] = None,
    ) -> str:
        """
        Builds a string XML request for the contracts endpoint using supported filters.
        """

        field_type: Dict[str, str] = {
            "fiscal_year": "value",
            "prime_vendor": "value",
            "vendor_code": "value",
            "contract_type": "value",
            "agency_code": "value",
            "contract_id": "value",
            "award_method": "value",
            "current_amount": "range",
            "start_date": "range",
            "end_date": "range",
            "registration_date": "range",
            "received_date": "range",
            "budget_name": "value",
            "commodity_line": "value",
            "entity_contract_number": "value",
            "other_government_entities_code": "value",
            "mwbe_category": "value",
            "industry": "value",
            "contract_includes_sub_vendors": "value",
            "sub_contract_status": "value",
            "purchase_order_type": "value",
            "approved_date": "value",
            "responsibility_center": "value",
            "conditional_category": "value",
            "contract_class": "value",
        }

        criteria: List[Criteria] = [
            {
                "name": "status",
                "type": "value",
                "value": status,
            },
            {
                "name": "category",
                "type": "value",
                "value": category,
            },
        ]

        if params:
            for key, value in params.items():
                if key not in field_type.keys():
                    closest_match = difflib.get_close_matches(word=key, possibilities=field_type.keys(), n=3, cutoff=0.2)
                    raise ValueError(f"Parameter: {key} is not valid. Closest potential matches: {closest_match}?")
                criteria.append(
                    {
                        "name": key,
                        "type": field_type[key],
                        "value": str(value),  # ensure value is string
                    }
                )

        xml_body = self._base_request(
            self.data_type, criteria, records_from, max_records, response_columns
        )
        return self._parse(self._post(xml_body).decode("utf-8"))

    def fetch_all(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        response_columns: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[str, int, float]]] = None,
    ):
        records_from = 1
        max_records = 20_000
        while True:
            records = self.fetch(
                status, category, records_from, max_records, response_columns, params
            )

            yield records

            if not records or len(records) < 20_000:
                logger.info("All records fetched.")
                break

            records_from += 20_000
