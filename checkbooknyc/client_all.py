from typing import Optional
import requests
from .payroll import Payroll
from .budget import Budget
from .contracts import Contracts
from .revenue import Revenue
from .spending import Spending


class CheckbookNYC:
    def __init__(
        self,
        base_url: str = "https://www.checkbooknyc.com/api",
        session: Optional[requests.Session] = None,
    ):
        if session is None:
            session = requests.Session()
        self.session = session
        self.base_url = base_url
        self.payroll = Payroll(self.session, self.base_url)
        self.budget = Budget(self.session, self.base_url)
        self.contracts = Contracts(self.session, self.base_url)
        self.revenue = Revenue(self.session, self.base_url)
        self.spending = Spending(self.session, self.base_url)
