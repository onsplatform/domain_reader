import ipdb
from domain.reader import QueryParser


def test_parse_array_parameter():
    # mock
    sql_filter = 'id in $ids'
    parameters = {'ids': [1, 2]}

    # act
    parser = QueryParser(sql_filter)
    query, params = parser.parse(parameters)

    # assert
    assert params == ((1, 2),)
    assert query == 'id in %s'


def test_parse_single_parameter():
    # mock
    sql_filter = 'id = :id'
    parameters = {'id': 1}

    # act
    parser = QueryParser(sql_filter)
    query, params = parser.parse(parameters)

    # assert
    assert params == (1, )
    assert query == 'id = %s'


def test_parse_optional_parameter():
    # mock
    sql_filter = 'col = val [and id = :id]'
    parameters = {'id': 1}

    # act
    parser = QueryParser(sql_filter)
    query, params = parser.parse(parameters)

    # assert
    assert params == (1,)
    assert query == 'col = val and id = %s'


def test_remove_optional_parameter():
    # mock
    sql_filter = 'col = val [and id = :id]'
    parameters = {}

    # act
    parser = QueryParser(sql_filter)
    query, params = parser.parse(parameters)

    # assert
    assert params == ()
    assert query == 'col = val'


def test_parse_optional_filter_with_no_parameters():
    sql_filter = '[id in ($ids)]'
    parameters = {}

    # act
    parser = QueryParser(sql_filter)
    query, params = parser.parse(parameters)

    # assert
    assert params == ()
    assert query == ''


