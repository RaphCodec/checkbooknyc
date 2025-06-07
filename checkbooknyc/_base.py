from typing import Any, Dict, List, Optional, TypedDict
from xml.etree import ElementTree as ET

import requests
from loguru import logger


class Criteria(TypedDict):
    name: str
    type: str
    value: str


class BaseClient:
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
        response_columns: Optional[List[str]] = None,
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
            <response_columns>

{"".join(f"                <column>{column}</column>\n" for column in response_columns) if response_columns else ""}
            </response_columns>
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
