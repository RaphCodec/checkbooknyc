from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from xml.etree import ElementTree as ET

import requests
from loguru import logger


class Criteria(TypedDict):
    name: str
    type: str
    value: str


class CheckbookNYCClient:
    def __init__(
        self,
        session: requests.Session,
        base_url: str = "https://www.checkbooknyc.com/api",
    ):
        self.base_url = base_url
        self.ses = session

    def _base_request(
        self,
        data_type: str,
        criteria: List[Criteria],
        records_from: Optional[int] = None,
        max_records: Optional[int] = None,
    ) -> str:
        criteria = criteria or []
        criteria_xml = "".join(
            [
                f"""
            <criteria>
                <name>{c["name"]}</name>
                <type>{c["type"]}</type>
                <value>{c["value"]}</value>
            </criteria>
            """
                for c in criteria
            ]
        )
        return f"""
        <request>
            <type_of_data>{data_type}</type_of_data>
            {f"<records_from>{records_from}</records_from>" if records_from else ""}
            {f"<max_records>{max_records}</max_records>" if max_records else ""}
            <search_criteria>{criteria_xml}</search_criteria>
        </request>
        """

    def _post(self, xml_request: str) -> bytes:
        response = self.ses.post(self.base_url, data=xml_request)
        response.raise_for_status()
        return response.content

    def _parse(self, xml_content: str) -> List[Dict[str, Any]] | str:
        try:
            root = ET.fromstring(xml_content)
            return [
                {child.tag: child.text for child in node}
                for node in root.findall(".//transaction")
            ]
        except Exception as e:
            logger.error(f"Failed to parse response: {e}")
            logger.error(f"Error: {xml_content}")
            return xml_content


class Contracts(CheckbookNYCClient):
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
        **filters: Dict[str, Union[str, int, float]],
    ) -> str:
        """
        Builds a string XML request for the contracts endpoint using supported filters.
        """

        field_type: Dict[str, str] = {
            "fiscal_year": "value",
            "status": "value",
            "category": "value",
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

        xml = self._base_request(self.data_type, criteria, records_from, max_records)
        return xml

    def fetch(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        records_from: Optional[int] = None,
        max_records: Optional[int] = None,
        get_all_records: bool = False,
        **filters: Dict[str, Union[str, int, float]],
    ):
        """
        Sends a POST request to the contracts endpoint with given filter criteria.
        """
        if get_all_records:
            return self._fetch_all_records(status, category, **filters)

        else:
            xml_body = self.build_contracts_request(
                status, category, records_from, max_records, **filters
            )
            return self._parse(self._post(xml_body).decode("utf-8"))

    def _fetch_all_records(
        self,
        status: Literal["active", "pending", "registered"],
        category: Literal["all", "expense", "revenue"],
        **filters: Dict[str, Union[str, int, float]],
    ):
        records_from = 1
        max_records = 20_000
        while True:
            xml_body = self.build_contracts_request(
                status, category, records_from, max_records, **filters
            )
            records = self._parse(self._post(xml_body).decode("utf-8"))
            yield records

            if not records or len(records) < 20_000:
                logger.info("No more records to fetch")
                break

            records_from += 20_000