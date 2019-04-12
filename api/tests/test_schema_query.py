import pytest

from api.schema.schema import *
from api.schema.schema_query import *


def test_create_field_with_name_and_alias():
    # arrange
    field_one = Field('nome_longo', 'nome')
    field_two = Field('nome_longo')

    # assert
    assert field_one.name == 'nome_longo'
    assert field_one.alias == 'nome'
    assert field_two.name == 'nome_longo'
    assert field_two.alias == 'nome_longo'


def test_create_filter_with_name_and_expression():
    # arrange
    filter_one = Filter('by_name', 'name = :name')

    # assert
    assert filter_one.name == 'by_name'
    assert filter_one.expression == 'name = :name'


def test_create_field_expression():
    # arrange
    field_one = Field('nome_longo', 'nome')

    # action
    expression = str(field_one)

    # assert
    assert expression == 'nome_longo as nome'


def test_create_filter_expression():
    # arrage
    filter_one = Filter('by_name', 'name = :name')

    # action
    expression = filter_one.get_expression()

    # assert
    assert expression == 'name = :name'


def test_create_schema_query_with_fields():
    # arrange
    field_one = Field('nome_longo', 'nome')
    field_two = Field('idade')
    fields = [field_one, field_two]
    model = 'pessoa'

    # action
    query = str(SchemaQuery(model, fields, None))

    # assert
    assert query == 'select nome_longo as nome, idade as idade from pessoa'


def test_create_schema_query_with_field():
    # arrange
    field_one = Field('idade')
    fields = [field_one]
    model = 'pessoa'

    # action
    query = str(SchemaQuery(model, fields, None))

    # assert
    assert query == 'select idade as idade from pessoa'


@pytest.mark.xfail(raises=TypeError)
def test_create_schema_query_with_parameter_less():
    # arrange

    # action
    query = str(SchemaQuery())

    # assert


@pytest.mark.xfail(raises=TypeError)
def test_create_schema_query_with_only_model():
    # arrange
    model = 'pessoa'

    # action
    query = str(SchemaQuery(model))

    # assert


@pytest.mark.xfail(raises=TypeError)
def test_create_schema_query_with_model_and_field():
    # arrange
    model = 'pessoa'
    field_one = Field('idade')
    fields = [field_one]

    # action
    query = str(SchemaQuery(model, fields))

    # assert


@pytest.mark.xfail(raises=ValueError)
def test_create_schema_query_with_model_none():
    # arrange
    field_one = Field('idade')
    fields = [field_one]

    # action
    query = str(SchemaQuery(None, fields, None))

    # assert


@pytest.mark.xfail(raises=ValueError)
def test_create_schema_query_with_field_none():
    # arrange
    model = 'pessoa'

    # action
    query = str(SchemaQuery(None, None, None))

    # assert


def test_create_schema_query_with_field_and_filter():
    # arrange
    field_one = Field('idade')
    fields = [field_one]
    model = 'pessoa'

    filter_one = Filter('byName', 'name = :name')

    # action
    query = str(SchemaQuery(model, fields, filter_one))

    # assert
    assert query == 'select idade as idade from pessoa where name = :name'

def test_create_schema_query_with_fields_and_filter():
    # arrange
    field_one = Field('nome_longo', 'nome')
    field_two = Field('idade')
    fields = [field_one, field_two]
    model = 'pessoa'

    filter_one = Filter('byName', 'name = :name')

    # action
    query = str(SchemaQuery(model, fields, filter_one))

    # assert
    assert query == 'select nome_longo as nome, idade as idade from pessoa where name = :name'
