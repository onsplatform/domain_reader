import falcon
import msgpack
import pytest
import requests
import requests_mock
import json
from falcon import testing
from api.app import api, domain_reader


@pytest.fixture
def client():
    return testing.TestClient(api)


def test_list_usinas(client):
    # arrange
    app = 'calculoteif'
    solution = 'sager'
    str_map = 'usinaDTO'
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"name": "nome_longo", "alias": "nome", "type": "str"},
            {"name": "desc", "alias": "descricao", "type": "str"}
        ],
        "filter": {"name": "byName", "expression": 'nome = :nome'}
    }

    # action
    with requests_mock.Mocker() as m:
        m.get(domain_reader.schema_api._get_schema_api_url(solution, app, str_map), status_code=200, json=api_response)
        response = client.simulate_get(
            '/reader/api/v1/{}/{}/{}/by_name/ITAUPU'.format(solution, app, str_map))
    response = json.loads(response.json)

    # assert
    assert len(response) == 2
    assert response[0]['nome'] == 'angra 1'
    assert response[1]['nome'] == 'angra 2'
    assert response[0]['descricao'] == 'descricao 1'
    assert response[1]['descricao'] == 'descricao 2'
