import os
import pytest

from platform_sdk.domain.reader.orms.peewee import Peewee
from platform_sdk.domain.writer import DomainWriter


@pytest.fixture(scope='class')
def db(request):
    return Peewee


@pytest.fixture(scope='class')
def db_settings():
    return {
        'database': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': os.environ.get('POSTGRES_HOST'),
        'port': os.environ.get('POSTGRES_PORT', 5432),
    }


@pytest.fixture(scope='class')
def process_memory_settings():
    return {
        'api_url': 'http://10.24.1.91/process_memory/',
    }


def test_get_schema(db, db_settings, process_memory_settings):
    # arrange
    domain_writer = DomainWriter(db, db_settings, process_memory_settings)
    content = {
        "tarefaretificacao": {
            "model": "tb_tarefa_retificacao",
            "fields": {
                "nome": {
                    "column": "nome",
                    "required": True
                },
                "situacao": {
                    "column": "situacao"
                }
            },
            "filters": {
                "byNomeTarefa": "nome = :nomeTarefa",
                "byId": "id = :id"
            }
        }
    }

    # action
    schema = domain_writer._get_schema(content)

    # assert
    assert schema['tarefaretificacao']['table'] == 'tb_tarefa_retificacao'
    assert schema['tarefaretificacao']['fields'][0]['name'] == 'nome'
    assert schema['tarefaretificacao']['fields'][0]['column'] == 'nome'
    assert schema['tarefaretificacao']['fields'][1]['name'] == 'situacao'
    assert schema['tarefaretificacao']['fields'][1]['column'] == 'situacao'


def test_get_update_sql(db, db_settings, process_memory_settings):
    # arrange
    domain_writer = DomainWriter(db, db_settings, process_memory_settings)
    content = {
        "tarefaretificacao": {
            "model": "tb_tarefa_retificacao",
            "fields": {
                "nome": {
                    "column": "nome",
                    "required": True
                },
                "situacao": {
                    "column": "situacao"
                }
            },
            "filters": {
                "byNomeTarefa": "nome = :nomeTarefa",
                "byId": "id = :id"
            }
        }
    }
    entity = {
        "_metadata": {
            "branch": "master",
            "created_at": "2018-08-30T22:04:18.000Z",
            "instance_id": "ecb2dff9-da9d-44b3-bcd1-9aac7338edea",
            "modified_at": "2018-08-30T22:04:18.000Z",
            "origin": None,
            "rid": "a9dd330e-dfc1-40c8-b7f5-621730b8ba14",
            "type": "tarefaretificacao",
            "queryInfo": {
                    "name": "tb_tarefa_retificacao",
                    "query": "nome = :nomeTarefa",
                    "filter": {
                        "nomeTarefa": "teste retificação set 2014"
                    }
            },
            "changeTrack": "update"
        },
        "id": "88769548-eaf9-4988-8ec2-f9662a23fb5b",
        "nome": "teste retificação set 2014",
        "situacao": "aplicado"
    }

    entity_id = '88769548-eaf9-4988-8ec2-f9662a23fb5b'
    schema = domain_writer._get_schema(content)
    table = schema['tarefaretificacao']['table']
    fields = schema['tarefaretificacao']['fields']

    # action
    sql = domain_writer._get_update_sql(entity_id, table, entity, fields)

    # asserts
    assert sql == 'update tb_tarefa_retificacao set nome=\'teste retificação set 2014\',situacao=\'aplicado\' where id=\'88769548-eaf9-4988-8ec2-f9662a23fb5b\';'


def test_get_insert_sql(db, db_settings, process_memory_settings):
    # arrange
    domain_writer = DomainWriter(db, db_settings, process_memory_settings)
    content = {
        "tarefaretificacao": {
            "model": "tb_tarefa_retificacao",
            "fields": {
                "nome": {
                    "column": "nome",
                    "required": True
                },
                "situacao": {
                    "column": "situacao"
                }
            },
            "filters": {
                "byNomeTarefa": "nome = :nomeTarefa",
                "byId": "id = :id"
            }
        }
    }
    entity = {
        "_metadata": {
            "branch": "master",
            "created_at": "2018-08-30T22:04:18.000Z",
            "instance_id": None,
            "modified_at": "2018-08-30T22:04:18.000Z",
            "origin": None,
            "rid": "a9dd330e-dfc1-40c8-b7f5-621730b8ba14",
            "type": "tarefaretificacao",
            "queryInfo": {
                    "name": "tb_tarefa_retificacao",
                    "query": "nome = :nomeTarefa",
                    "filter": {
                        "nomeTarefa": "teste retificação set 2014"
                    }
            },
            "changeTrack": "update"
        },
        "id": "88769548-eaf9-4988-8ec2-f9662a23fb5b",
        "nome": "teste retificação set 2014",
        "situacao": "aplicado"
    }

    schema = domain_writer._get_schema(content)
    table = schema['tarefaretificacao']['table']
    fields = schema['tarefaretificacao']['fields']
    columns = (field['column'] for field in fields)

    # action
    sql = domain_writer._get_insert_sql(table, entity, fields)

    # assert
    assert sql == 'insert into tb_tarefa_retificacao (nome,situacao) values (\'teste retificação set 2014\',\'aplicado\');'


def test_get_sql(db, db_settings, process_memory_settings):
    # arrange
    domain_writer = DomainWriter(db, db_settings, process_memory_settings)
    data = {
        "event": {
            "instanceId": "00818ce2-6263-4a26-b8b4-b69737019319",
        },
        "map": {
            "_metadata": {
                "branch": "master",
                "created_at": "2018-08-28T22:05:33.000Z",
                "instance_id": None,
                "modified_at": "2018-08-28T22:05:33.000Z",
                "origin": None,
                "rid": "de628f29-8be7-42ca-aa85-fbc3ee2eae99",
                "type": "map"
            },
            "content": {
                "tarefaretificacao": {
                    "model": "tb_tarefa_retificacao",
                    "fields": {
                        "nome": {
                            "column": "nome",
                            "required": True
                        },
                        "situacao": {
                            "column": "situacao"
                        }
                    },
                    "filters": {
                        "byNomeTarefa": "nome = :nomeTarefa",
                        "byId": "id = :id"
                    }
                },
                "unidadegeradora": {
                    "model": "tb_unidade_geradora",
                    "fields": {
                        "idUge": {
                            "column": "id_uge"
                        },
                        "potenciaDisponivel": {
                            "column": "pot_disp"
                        },
                        "dataInicioOperacao": {
                            "column": "data_inicio_operacao"
                        },
                        "idUsina": {
                            "column": "id_usina"
                        }
                    },
                    "filters": {
                        "byIdUsina": "id_usina in ($idsUsinas)",
                        "all": None
                    }
                },
            }
        },
        "dataset": {
            "entities": {
                "tarefaretificacao": [
                        {
                            "_metadata": {
                                "branch": "master",
                                "created_at": "2018-08-30T22:04:18.000Z",
                                "instance_id": "ecb2dff9-da9d-44b3-bcd1-9aac7338edea",
                                "modified_at": "2018-08-30T22:04:18.000Z",
                                "origin": None,
                                "rid": "a9dd330e-dfc1-40c8-b7f5-621730b8ba14",
                                "type": "tarefaretificacao",
                                "queryInfo": {
                                    "name": "tb_tarefa_retificacao",
                                    "query": "nome = :nomeTarefa",
                                    "filter": {
                                        "nomeTarefa": "teste retificação set 2014"
                                    }
                                },
                                "changeTrack": "update"
                            },
                            "id": "88769548-eaf9-4988-8ec2-f9662a23fb5b",
                            "nome": "teste retificação set 2014",
                            "situacao": "aplicado"
                        }, {
                            "_metadata": {
                                "branch": "master",
                                "created_at": "2018-08-30T22:02:40.000Z",
                                "instance_id": "76c43e02-2270-4c45-b44a-8a3a2c7c7c47",
                                "modified_at": "2018-08-30T22:18:41.000Z",
                                "origin": None,
                                "rid": "a5c94aa9-991e-47bc-8c08-e9fd38443672",
                                "type": "tarefaretificacao",
                                "queryInfo": {
                                    "name": "tb_tarefa_retificacao",
                                    "filter": {
                                        "nomeTarefa": "teste retificação set 2014"
                                    }
                                }
                            },
                            "id": "b228317b-1e10-4ed2-bf1a-41d5acdbe7e8",
                            "nome": "teste retificação set 2014",
                            "situacao": "iniciada"
                        }
                ],
                "unidadegeradora": [
                    {
                        "_metadata": {
                            "branch": "master",
                            "created_at": "2018-08-28T22:08:36.000Z",
                            "instance_id": None,
                            "modified_at": "2018-08-28T22:08:36.000Z",
                            "origin": None,
                            "rid": "53d84816-db34-4575-9399-2d85273e0c1f",
                            "type": "unidadegeradora",
                            "queryInfo": {
                                    "name": "tb_unidade_geradora",
                                    "query": None,
                                    "filter": {}
                            }
                        },
                        "dataInicioOperacao": "1997-08-22T00:00:00.000Z",
                        "id": "8954ceaa-c31a-4b7f-8c12-8efb7e05a8ed",
                        "idUge": "ALUXG-0UG1",
                        "idUsina": "ALUXG",
                        "potenciaDisponivel": 527
                    },
                    {
                        "_metadata": {
                            "branch": "master",
                            "created_at": "2018-08-28T22:08:36.000Z",
                            "instance_id": None,
                            "modified_at": "2018-08-28T22:08:36.000Z",
                            "origin": None,
                            "rid": "0d8660a2-8a85-4f7a-a101-d0c06020d143",
                            "type": "unidadegeradora",
                            "queryInfo": {
                                    "name": "tb_unidade_geradora",
                                    "filter": {}
                            }
                        },
                        "dataInicioOperacao": "1996-12-20T00:00:00.000Z",
                        "id": "c2e6b1f1-3af6-4cb4-81a1-a1cb2b81c609",
                        "idUge": "ALUXG-0UG2",
                        "idUsina": "ALUXG",
                        "potenciaDisponivel": 527
                    }
                ]
            }
        }
    }

    # action
    schema = domain_writer._get_schema(data['map']['content'])
    bulk_sql = domain_writer._get_sql(data['dataset']['entities'], schema)

    # assert
    assert len(bulk_sql) == 4
    assert bulk_sql == [
        "update tb_tarefa_retificacao set nome='teste retificação set 2014',situacao='aplicado' where id='88769548-eaf9-4988-8ec2-f9662a23fb5b';",
        "update tb_tarefa_retificacao set nome='teste retificação set 2014',situacao='iniciada' where id='b228317b-1e10-4ed2-bf1a-41d5acdbe7e8';",
        "insert into tb_unidade_geradora (id_uge,pot_disp,data_inicio_operacao,id_usina) values ('ALUXG-0UG1','527','1997-08-22T00:00:00.000Z','ALUXG');",
        "insert into tb_unidade_geradora (id_uge,pot_disp,data_inicio_operacao,id_usina) values ('ALUXG-0UG2','527','1996-12-20T00:00:00.000Z','ALUXG');"
    ]
