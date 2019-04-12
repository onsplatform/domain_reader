import pytest
import requests
import requests_mock

from api.schema.schema_api import *


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


def test_get_fields():
    # arrange
    fields_dict = [{'name': 'field_1', 'alias': 'alias_1'}]

    # action
    schema_api = SchemaApi()
    fields = schema_api._get_fields(fields_dict)

    # assert
    assert len(fields) == 1


def test_get_filter():
    # arrange
    filter_dict = {"name": "byName", "expression": 'name = :name'}

    # action
    schema_api = SchemaApi()
    filter_obj = schema_api._get_filter(filter_dict)

    # assert
    assert filter_obj.get_expression() == 'name = :name'
    assert filter_obj.name == 'byName'

def test_get_schema():
    # arrange
    app = 'teif'
    solution = 'sager'
    str_map = 'usinaDTO'
    schema_api = SchemaApi()
    api_response = {
            "model": "usina",
            "fields": [
                {"name": "nome_longo", "alias": "nome"}
            ],
            "filter": {"name": "byName", "expression": 'nome = :nome'}
        }
    sql = 'select nome_longo as nome from usina where nome = :nome'

    # action
    with requests_mock.Mocker() as m:
        m.get(schema_api._get_schema_api_url(
            solution, app, str_map), status_code=200, json=api_response)
        response = schema_api.get_schema(solution, app, str_map)

    # assert
    assert str(response) == sql