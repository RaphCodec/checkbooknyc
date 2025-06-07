import checkbooknyc as ck
from requests import Session

def test_contracts_init():
    session = Session()
    contracts = ck.Contracts(session=session)
    assert contracts.data_type == "Contracts"
