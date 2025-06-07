import checkbooknyc as ck
from requests import Session

def test_contracts_init():
    session = Session()
    spending = ck.Spending(session=session)
    assert spending.data_type == "Spending"
