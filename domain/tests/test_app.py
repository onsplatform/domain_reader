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


def test_list_usinas(client):
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

    domain_reader._execute_query = Mock(return_value=list(
        [Usina(1, 'angra 1', 'descricao 1'), Usina(2, 'angra 2', 'descricao 2')]))

    # action
    with requests_mock.Mocker() as m:
        m.get(domain_reader.schema_api.get_uri(str_map, str_type),
              status_code=200, json=api_response)
        response = client.simulate_get(
            '/reader/api/v1/{}/x/byName?nomes=ITAUPU'.format(str_map, str_type))
    response = response.json

    # assert
    assert len(response) == 2
    assert response[0]['nome'] == 'angra 1'
    assert response[1]['nome'] == 'angra 2'


def test_list_entities_with_no_result(client):
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
            '/reader/api/v1/{}/{}/byName?nomes=ITAUPU'.format(str_map, str_type))

    # assert
    assert response.status_code == 404


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
