import checkbooknyc as ck
from requests import Session

def test_contracts_init():
    session = Session()
    budget = ck.Budget(session=session)
    assert budget.data_type == "Budget"
