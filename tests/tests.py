import logging
from contextlib import contextmanager
from magic_generator import generator as g
from magic_generator import utilities as u
import unittest.mock
import pytest
import json
from multiprocessing import Value


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize('schema, if_raises_exception, expected_output', [
    ('{"str": "str:name"}', does_not_raise(), "Schema OK"),
    ('{"timestamp": "timestamp:"}', does_not_raise(), "Schema OK"),
    ('{"int": "int:50"}', does_not_raise(), "Schema OK"),
    ('{"wrong value": "bool:"}', pytest.raises(SystemExit), "not a valid type")
])
def test_types_parse_data_schema(schema, if_raises_exception, expected_output, caplog):
    with caplog.at_level(logging.INFO):
        with if_raises_exception:
            g.get_data_schema(schema)
    assert expected_output in caplog.text


@pytest.mark.parametrize('schema, exception_raised, expected', [
    ('{"name": "str:name"}', False, {'name': 'return_const_value("name")'}),
    ('{"date": "timestamp:"}', False, {'date': 'time.time()'}),
    ('{"age": "int:50"}', False, {'age': 'return_const_value(50)'}),
    ('{"empty string": "str:"}', False, {'empty string': 'return_const_value("")'}),
    ('{"empty int": "int:"}', False, {'empty int': 'return_const_value(None)'}),
    ('{"random string": "str:rand"}', False, {'random string': 'str(uuid.uuid4())'}),
    ('{"random int": "int:rand"}', False, {'random int': 'random.randint(0, 10000)'}),
    ('{"choice int": "int:[1, 3, 9]"}', False, {"choice int": "random.choice([1, 3, 9])"}),
    ('{"choice str": "str:[\'client\', \'government\']"}', False,
     {'choice str': "random.choice(['client', 'government'])"}),
    ('{"rand from-to": "int:rand(1, 1000)"}', False, {'rand from-to': 'random.randint(1, 1000)'}),
    ('{"str rand from to": "str:rand(1, 1000)"}', True, "not a valid value"),
    ('{"wrong int value": "int:name"}', True, "not a valid value"),
    ('{"wrong list": "int:[\'client\', \'government\']"}', True, "not a valid value"),
    ('wrong json format', True, "not a valid json format"),
    ('{"wrong format": "intrand"}', True, "not a valid schema format")
])
def test_schema_parse_data_schema(schema, exception_raised, expected, caplog):

    if exception_raised:
        with caplog.at_level(logging.INFO):
            with pytest.raises(SystemExit):
                g.get_data_schema(schema)
            assert expected in caplog.text
    else:
        assert g.get_data_schema(schema) == expected


@pytest.fixture(scope='function')
def json_file(tmp_path, request):
    f = tmp_path.joinpath('temp_file.json')
    f.write_text(request.param, encoding='utf-8')
    return f


@pytest.mark.parametrize('json_file, raises_exception, expected', [
    ('{"name": "str:name"}', False, {"name": "str:name"}),
    ('{"name": "str"', True, 'not a valid json format')
], indirect=['json_file'])
def test_schema_from_file(json_file, raises_exception, expected, caplog):
    if raises_exception:
        with caplog.at_level(logging.INFO):
            with pytest.raises(SystemExit):
                g.get_json_object(str(json_file))
                assert expected in caplog.text
    else:
        assert g.get_json_object(str(json_file)) == expected


def test_schema_from_unexisting_file(tmp_path, caplog):
    with caplog.at_level(logging.INFO):
        with pytest.raises(SystemExit):
            g.get_json_object(str(tmp_path/'some_file.json'))
    assert 'does not exist' in caplog.text


@pytest.mark.parametrize('files_name, files_to_clear, expected, exp_log', [
    (['test_file_1', 'test_file_2'], 'test_file', 0, 'finished'),
    (['test_file'], 'test_file', 0, 'finished'),
    (['test_file', 'another_test_file'], 'test_file', 1, 'finished'),
    (['test_file_1', 'test_file_2'], 'another_test_file', 2, 'No files to clear'),
    (['test_file1234'], 'test_file', 1, 'No files to clear'),
    (['test_file_j3b34-hjeb2-weqewqwe-djqwew'], 'test_file', 0, 'finished')
])
def test_clear_path(tmp_path, files_name, files_to_clear, expected,
                    exp_log, caplog):
    for file in files_name:
        f1 = tmp_path.joinpath(f'{file}.jsonl')
        f1.touch()
    with caplog.at_level(logging.INFO):
        u.clear_path(str(tmp_path), files_to_clear)
    assert len(list(tmp_path.iterdir())) == expected
    assert exp_log in caplog.text


@unittest.mock.patch('magic_generator.generator.generate_data_line',
                     return_value={"name": "random_name", "age": 55})
def test_saving_file(mocked_generator, tmp_path):
    u.counter = Value('i', 0)
    json_file = tmp_path.joinpath('test_file')
    u.write_data_to_file(str(json_file), "", {"some key": "some value"}, 1, 1)
    expected = {"name": "random_name", "age": 55}
    with open(f'{json_file}.jsonl', 'r') as f:
        actual = json.load(f)
    assert len(list(tmp_path.iterdir())) == 1
    assert actual == expected


def test_writing_data_to_files_concurrently(tmp_path):
    filepath = tmp_path.joinpath('test_file')
    data_schema = {"name": "str(uuid.uuid4())", "age": "random.randint(1, 50)"}
    u.write_data_to_files_concurrently(2, 1000, str(filepath),
                                      'index + 1', data_schema, 1000)
    assert len(list(tmp_path.iterdir())) == 1000


sample_dict = {"name": "random_name", "age": 55}


@unittest.mock.patch('magic_generator.generator.get_data_lines',
                     return_value=[sample_dict, sample_dict, sample_dict])
def test_printing_to_console(mocked_generator, capfd):
    u.print_to_console({"some key": "some value"}, 3)
    out, err = capfd.readouterr()
    assert out == "{'name': 'random_name', 'age': 55}\n" * 3
