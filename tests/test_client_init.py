import checkbooknyc as ck
from requests import Session


def test_budget_init():
    session = Session()
    budget = ck.Budget(session=session)
    assert budget.data_type == "Budget"


def test_contracts_init():
    session = Session()
    contracts = ck.Contracts(session=session)
    assert contracts.data_type == "Contracts"


def test_payroll_init():
    session = Session()
    payroll = ck.Payroll(session=session)
    assert payroll.data_type == "Payroll"


def test_revenue_init():
    session = Session()
    revenue = ck.Revenue(session=session)
    assert revenue.data_type == "Revenue"


def test_spending_init():
    session = Session()
    spending = ck.Spending(session=session)
    assert spending.data_type == "Spending"
