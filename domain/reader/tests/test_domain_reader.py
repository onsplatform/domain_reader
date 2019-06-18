from platform_sdk.domain.reader import DomainReader


def test_init_domain_reader(db, schema_settings, db_settings):
    # arrange
    domain_reader = DomainReader(db, db_settings, schema_settings)

    # assert
    assert domain_reader.schema_api is not None


def test_get_model(db, schema_settings, db_settings):
    # arrange
    domain_reader = DomainReader(db, db_settings, schema_settings)
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"column_name": "nome_longo", "alias": "nome", "field_type": "str"},
            {"column_name": "desc", "alias": "descricao", "field_type": "str"}
        ],
        "filter": {"name": "byName", "expression": 'nome = :nome'}
    }

    # action
    model = domain_reader._get_model(
        api_response['model'], api_response['fields'])

    # assert
    assert model.name == "Usina"
    assert model.table == "tb_usina"
    assert model.fields[0].name == "nome"
    assert model.fields[0].field_type == "str"
    assert model.fields[0].column_name == "nome_longo"


def test_get_fields(db, schema_settings, db_settings):
    # arrange
    fields_dict = [{'column_name': 'field_1',
                    'alias': 'alias_1', "field_type": "str"}]

    # action
    domain_reader = DomainReader(db, db_settings, schema_settings)
    fields = domain_reader._get_fields(fields_dict)

    # assert
    assert len(fields) == 1


'''
def test_get_data_with_no_api_response(db):
    # arrange
    app = 'teif'
    solution = 'sager'
    _map = 'usinaDTO'
    domain_reader = DomainReader(db)

    # action
    data = domain_reader.get_data(solution, app, _map)

    # assert
    assert data == None
'''


def test_get_response_data(db, schema_settings, db_settings):
    # arrange
    domain_reader = DomainReader(db, db_settings, schema_settings)
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"column_name": "nome_longo", "alias": "nome", "field_type": "str"},
            {"column_name": "desc", "alias": "descricao", "field_type": "str"}
        ],
        "filter": {"name": "byName", "expression": 'nome = :nome'}
    }

    # action
    class Usina:
        def __init__(self, nome, descricao):
            self.nome = nome
            self.descricao = descricao

    usina1 = Usina('angra 1', 'descricao 1')
    usina2 = Usina('angra 2', 'descricao 2')
    data = domain_reader._get_response_data(
        list([usina1, usina2]), api_response['fields'])

    # assert
    assert data[0]['nome'] == 'angra 1'
    assert data[0]['descricao'] == 'descricao 1'

    assert data[1]['nome'] == 'angra 2'
    assert data[1]['descricao'] == 'descricao 2'


def test_get_response_data_empty(db, schema_settings, db_settings):
    # arrange
    domain_reader = DomainReader(db, db_settings, schema_settings)
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"column_name": "nome_longo", "alias": "nome", "field_type": "str"},
            {"column_name": "desc", "alias": "descricao", "field_type": "str"}
        ],
        "filter": {"name": "byName", "expression": 'nome = :nome'}
    }

    # action
    class Usina:
        def __init__(self, nome, descricao):
            self.nome = nome
            self.descricao = descricao

    usina1 = Usina('angra 1', 'descricao 1')
    usina2 = Usina('angra 2', 'descricao 2')
    data = domain_reader._get_response_data(
        list([]), api_response['fields'])

    # assert
    assert not data


def test_execute_query(db, schema_settings, db_settings):
    # arrange
    domain_reader = DomainReader(db, db_settings, schema_settings)
    api_response = {
        "model": {"name": "Usina", "table": "tb_usina"},
        "fields": [
            {"column_name": "nome_longo", "alias": "nome", "field_type": "str"}
        ],
        "filters": [
            {"name": "byName", "expression": 'nome_longo in $nomes and nome_longo != :nome1'},
            {"name": "byIds", "expression": "id in $ids"}
        ]
    }

    params = {
        'ids': [1, 2, ]
    }

    model = domain_reader._get_model(
        api_response['model'], api_response['fields'])
    sql_filter = domain_reader._get_sql_filter(
        'byIds', api_response['filters'])
    sql_query = domain_reader._get_sql_query(sql_filter, params)

    # action
    data = domain_reader._execute_query(model, sql_query)

    # assert
    assert data[0].nome == 'ITAUPU'
