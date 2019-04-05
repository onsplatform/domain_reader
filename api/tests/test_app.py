import falcon
from falcon import testing
import msgpack
import pytest
from api.app import api


@pytest.fixture
def client():
    return testing.TestClient(api)


def test_list_usinas(client):
    doc = {
        'usinas': [
            {
                'name': 'ITAIPU'
            }
        ]
    }

    response = client.simulate_get('/reader/api/v1/sager/calculoteif/usidadto/by_name/?q=ITAUPU')
    result_doc = msgpack.unpackb(response.content, encoding='utf-8')

    assert result_doc == doc
    assert response.status == falcon.HTTP_OK
