'''
import falcon
import msgpack
import pytest
from api.app import api
from falcon import testing


@pytest.fixture
def client():
    return testing.TestClient(api)

def test_list_usinas(client):
    doc = {
        'usinas': [
            {
                'name': 'ITAUPU'
            }
        ]
    }

    response = client.simulate_get(
        '/reader/api/v1/sager/calculoteif/usidadto/by_name/ITAUPU')
    result_doc = msgpack.unpackb(response.content, raw=False)

    assert result_doc == doc
    assert response.status == falcon.HTTP_OK
'''