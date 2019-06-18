import mock

from platform_sdk.domain.reader.orms.peewee import Peewee
from platform_sdk.domain.reader.mapper import RemoteField, RemoteMap



def test_remote_field():
    # arrange
    remote_field = RemoteField('nome', str, 'nome_longo', False)

    # assert
    assert remote_field.name == 'nome'
    assert remote_field.column_name == 'nome_longo'
    assert remote_field.field_type == str
    assert remote_field.required == False


def test_remote_map():
    # arrange
    remote_field = RemoteField('nome', 'str', 'nome_longo')
    remote_map = RemoteMap('Usina', 'tb_usina', [remote_field], orm=Peewee)

    # assert
    assert remote_map.name == 'Usina'
    assert remote_map.table == 'tb_usina'
    assert len(remote_map.fields) == 1


def test_remote_map_build(db):
    # arrange
    remote_field = RemoteField('nome', 'str', 'nome_longo')
    remote_map = RemoteMap('Usina', 'tb_usina', [remote_field], orm=Peewee)

    # action
    ret = remote_map.build(database=db)

    # assert
    assert ret.__name__ == 'Usina'
    assert hasattr(ret, 'id')
    assert hasattr(ret, 'nome')
    assert ret._meta.table_name == 'tb_usina'


