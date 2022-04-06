import os
import re
import logging
import json
import random
import uuid
import multiprocessing as mp
from magic_generator import generator as generator
from magic_generator.logger import logger_error


global counter


def get_prefix(prefix: str) -> str:
    if prefix == 'count':
        file_prefix = 'index + 1'
    elif prefix == 'random':
        file_prefix = 'random.randint(1, 10000)'
    else:
        file_prefix = 'uuid.uuid4()'
    return file_prefix


def get_output_dir_path(dir_path: str) -> str:
    """Get absolute output path. Error if path exists and is not directory."""
    if not os.path.isabs(dir_path):
        dir_path = os.path.abspath(dir_path)
    if os.path.exists(dir_path) and not os.path.isdir(dir_path):
        logger_error(5, dir_path)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    return dir_path


def clear_path(path: str, filename: str) -> None:
    """Remove all files in path that match filename."""
    pattern = f'{filename}(_[0-9A-Za-z\-]+)?\.jsonl\Z'
    all_files = [os.path.join(path, file) for file in os.listdir(path)
                 if re.match(pattern, file)]
    logging.info('Clearing output path...')
    for file in all_files:
        logging.info(f'Removing file {file}...')
        try:
            os.remove(file)
        except OSError:
            logger_error(7, file)
    if len(all_files) == 0:
        logging.info('No files to clear.')
    else:
        logging.info('Clearing output path finished.')


def print_to_console(data_schema: dict, data_lines: int) -> None:
    """Generate data lines and print them to console"""
    logging.info('Generating data started...')
    data = generator.get_data_lines(data_schema, data_lines)
    logging.info('Generating data finished. Printing to console.')
    print(*data, sep='\n')


def write_data_to_file(filepath: str, file_prefix: str, data_schema: dict,
                       data_lines: int, files_count: int) -> None:
    """Generate data lines and save them to file"""
    global counter
    with counter.get_lock():
        index = counter.value
        counter.value += 1
    prefix = f'_{eval(file_prefix)}.jsonl' \
        if files_count > 1 else '.jsonl'
    full_filepath = filepath + prefix
    logging.info(f'Generating data {index + 1}/{files_count}...')
    data = generator.get_data_lines(data_schema, data_lines)
    logging.info(f'Writing data to file {full_filepath}')
    try:
        with open(full_filepath, 'w') as f:
            for line in data:
                json.dump(line, f)
                f.write('\n')
    except IOError:
        logger_error(8, full_filepath)
    else:
        logging.info(f'Writing data to file {full_filepath} finished.')


def write_data_to_files(files_count: int, filepath: str, file_prefix: str,
                        data_schema: dict, data_lines: int) -> None:
    """Write data to `files count` number of files."""
    global counter
    init_counter(mp.Value('i', 0))
    for _ in range(files_count):
        write_data_to_file(filepath, file_prefix, data_schema,
                           data_lines, files_count)


def write_data_to_files_concurrently(process_count: int, files_count: int,
                                     filepath: str, file_prefix: str,
                                     data_schema: dict, data_lines: int) -> None:
    """Create pool of processes and write data to `files_count`
    number of files concurrently."""
    count = mp.Value('i', 0)
    with mp.Pool(processes=process_count,
                 maxtasksperchild=files_count//process_count,
                 initializer=init_counter, initargs=(count, )) as pool:
        params = [
            (filepath, file_prefix, data_schema, data_lines, files_count)
            for _ in range(files_count)]
        pool.starmap(write_data_to_file, params)


def init_counter(val: mp.Value) -> None:
    """Helper function for initializing counter between processes."""
    global counter
    counter = val
