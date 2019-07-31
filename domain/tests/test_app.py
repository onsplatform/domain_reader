import json
import falcon
import pytest
import requests
import requests_mock

from mock import Mock
from falcon import testing
from domain.app import domain_reader, api


class Usina:
    def __init__(self, id, nome, descricao):
        self.id = id
        self.nome = nome
        self.descricao = descricao


@pytest.fixture
def client():
    return testing.TestClient(api)


def test_list_entities_with_no_parameters(client):
    # arrange
    app = 'calculoteif'
    solution = 'sager'
    str_map = 'sager'
    str_type = 'usinaDTO'
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"column_name": "id", "alias": "id", "field_type": "int"},
            {"column_name": "nome_longo", "alias": "nome", "field_type": "str"}
        ],
        "filters": [
            {"name": "byName", "expression": "nome_longo in $nomes"},
            {"name": "byIds", "expression": "id in $ids"}
        ]
    }
    domain_reader._execute_query = Mock(return_value=list([]))

    # action
    with requests_mock.Mocker() as m:
        m.get(domain_reader.schema_api.get_uri(str_map, str_type),
              status_code=200, json=api_response)
        response = client.simulate_get(
            '/reader/api/v1/{}/{}/byName?nomes=ITAUPU'.format('', '', ''))

    # assert
    assert response.status_code == 400
