from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from xml.etree import ElementTree as ET

import requests
from loguru import logger
from .client import CheckbookNYC, Criteria




class Contracts(CheckbookNYC):
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        super().__init__(session, base_url)
        self.data_type = "Contracts"

    def build_contracts_request(
        self,
        status: str,
        category: str,
        records_from: Optional[int],
        max_records: Optional[int],
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
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

        for key, value in filters.items():
            if key not in field_type.keys():
                logger.warning(f"Key: {key} is not valid and will be ignored.")
                continue
            criteria.append(
                {
                    "name": key,
                    "type": field_type[key],
                    "value": str(value),  # ensure value is string
                }
            )

        xml = self._base_request(self.data_type, criteria, records_from, max_records, response_columns)
        return xml

    def fetch(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        records_from: Optional[int] = None,
        max_records: Optional[int] = None,
        get_all_records: bool = False,
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        """
        Sends a POST request to the contracts endpoint with given filter criteria.
        """
        if get_all_records:
            return self._fetch_all_records(status, category, response_columns, **filters)

        else:
            xml_body = self.build_contracts_request(
                status, category, records_from, max_records, response_columns, **filters
            )
            return self._parse(self._post(xml_body).decode("utf-8"))

    def _fetch_all_records(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        response_columns: Optional[List[str]] = None,
        **filters: Dict[str, Union[str, int, float]],
    ):
        records_from = 1
        max_records = 20_000
        while True:
            xml_body = self.build_contracts_request(
                status, category, records_from, max_records, response_columns, **filters
            )
            
            records = self._parse(self._post(xml_body).decode("utf-8"))
            yield records

            if not records or len(records) < 20_000:
                logger.info("No more records to fetch")
                break

            records_from += 20_000
