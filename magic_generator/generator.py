from typing import Union
import os
import json
import logging
import random
import time
import uuid
import re
from magic_generator import ACCEPTED_VALUES
from magic_generator.logger import logger_error


def get_json_object(input_string: str) -> dict:
    """Returns valid json dict from schema in input string or json file.
    Error if file doesn't exist or input schema is not valid json format."""

    if input_string.endswith('.json'):
        if not os.path.isabs(input_string):
            input_string = os.path.abspath(input_string)
        try:
            with open(input_string, 'r') as f:
                schema = json.load(f)
        except IOError:
            logger_error(0, input_string)
        except json.decoder.JSONDecodeError:
            logger_error(1, input_string)
    else:
        try:
            schema = json.loads(input_string)
        except json.decoder.JSONDecodeError:
            logger_error(1, input_string)

    return schema


def get_data_schema(input_string: str) -> dict:
    """Parse data schema based on types and values
    provided in input string or json file.
    Returns valid schema for generating json lines.
    Error if any part of schema is not valid."""

    schema = get_json_object(input_string)

    logging.info('Schema parsing started...')

    for key, value in schema.items():
        try:
            v_type, v_to_generate = value.split(':')
        except ValueError:
            logger_error(6, key, value)
        if v_type not in ACCEPTED_VALUES:
            logger_error(3, v_type)
        elif v_type == 'timestamp':
            if v_to_generate != '':
                logging.warning(f'Timestamp type does not accept any values.'
                                f'The value {v_to_generate} will be ignored.')
            schema[key] = 'time.time()'
        elif v_to_generate == '':
            schema[key] = 'return_const_value(None)' \
                if v_type == 'int' else 'return_const_value("")'
        elif v_to_generate == 'rand':
            if v_type == 'int':
                schema[key] = 'random.randint(0, 10000)'
            elif v_type == 'str':
                schema[key] = 'str(uuid.uuid4())'
        elif v_to_generate[0] == '[' and v_to_generate[-1] == ']':
            if v_type == 'int':
                try:
                    v_list = [int(v) for v in v_to_generate[1:-1].split(', ')]
                except ValueError:
                    logger_error(2, v_to_generate, v_type)
                schema[key] = f'random.choice({v_list})'
            elif v_type == 'str':
                v_list = [v.strip("\\'") for v in v_to_generate[1:-1].split(', ')]
                schema[key] = f'random.choice({v_list})'
        elif re.match(r'rand\([0-9]+, {0,1}[0-9]+\)\Z', v_to_generate):
            if v_type == 'int':
                from_, to_ = (int(v.strip()) for v
                              in v_to_generate
                              .split('(')[-1].strip(')').split(','))
                schema[key] = f'random.randint({from_}, {to_})'
            elif v_type == 'str':
                logger_error(2, v_to_generate, v_type)
        else:
            if v_type == 'int':
                try:
                    int(v_to_generate)
                except ValueError:
                    logger_error(2, v_to_generate, v_type)
                else:
                    schema[key] = f'return_const_value({v_to_generate})'
            elif v_type == 'str':
                schema[key] = f'return_const_value("{v_to_generate}")'

    logging.info('Schema parsing finished. Schema OK.')

    return schema


def return_const_value(value: Union[str, int, None]) -> Union[str, int, None]:
    """Helper function for generating const data with eval()."""
    return value


def generate_data_line(schema: dict) -> dict:
    """Generate one line of data based on valid schema."""
    data = dict()
    for key, value in schema.items():
        data[key] = eval(value)
    return data


def get_data_lines(schema: dict, lines: int) -> list:
    """Get all lines of data for one json file."""
    data_lines = list()
    for _ in range(lines):
        data_line = generate_data_line(schema)
        data_lines.append(data_line)
    return data_lines
