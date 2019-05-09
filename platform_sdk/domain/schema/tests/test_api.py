import pytest
import requests
import requests_mock


from schema.api import SchemaApi


def test_get_schema():
    # arrange
    app = 'teif'
    solution = 'sager'
    _map = 'usinaDTO'
    schema_api = SchemaApi()
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
        m.get(schema_api._get_schema_api_url(solution, app, _map),
              status_code=200, json=api_response)

        response = schema_api.get_schema(solution, app, _map)

    # assert
    assert response['model']['name'] == 'Usina'
    assert response['model']['table'] == 'tb_usina'
    assert response['fields'][0]['name'] == 'nome_longo'
    assert response['fields'][0]['alias'] == 'nome'
    assert response['fields'][0]['type'] == 'str'


def test_get_schema_response_with_body():
    # arrange
    app = 'teif'
    solution = 'sager'
    str_map = 'usinaDTO'
    schema_api = SchemaApi()

    # action
    with requests_mock.Mocker() as m:
        m.get(schema_api._get_schema_api_url(
            solution, app, str_map), text='resp')
        response = schema_api._get_schema_response(solution, app, str_map)

    # assert
    assert response is not None


def test_get_schema_response_with_no_body():
    # arrange
    app = 'teif'
    solution = 'sager'
    str_map = 'usinaDTO'
    schema_api = SchemaApi()

    # action
    with requests_mock.Mocker() as m:
        m.get(schema_api._get_schema_api_url(
            solution, app, str_map), status_code=400)
        response = schema_api._get_schema_response(solution, app, str_map)

    # assert
    assert response is None

def test_get_schema_none():
    # arrange
    app = 'teif'
    solution = 'sager'
    str_map = 'usinaDTO'
    schema_api = SchemaApi()

    # action
    with requests_mock.Mocker() as m:
        m.get(schema_api._get_schema_api_url(
            solution, app, str_map), status_code=400)
        response = schema_api._get_schema_response(solution, app, str_map)
        schema = schema_api.get_schema(solution, app, str_map)

    # assert
    assert schema is None



def test_get_url_schema_api():
    # arrange
    app = 'teif'
    solution = 'sager'
    str_map = 'usinaDTO'
    schema_api = SchemaApi()
    url_test = "http://localhost/schema/sager/teif/usinaDTO"

    # action
    url = schema_api._get_schema_api_url(solution, app, str_map)

    # assert
    assert url == url_test
