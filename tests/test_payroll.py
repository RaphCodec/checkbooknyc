import checkbooknyc as ck
from requests import Session

def test_contracts_init():
    session = Session()
    payroll = ck.Payroll(session=session)
    assert payroll.data_type == "Payroll"
