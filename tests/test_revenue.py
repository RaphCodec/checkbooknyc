import checkbooknyc as ck
from requests import Session

def test_contracts_init():
    session = Session()
    revenue = ck.Revenue(session=session)
    assert revenue.data_type == "Revenue"
