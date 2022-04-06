import argparse
import os
import logging
from magic_generator import config as c
from magic_generator import utilities as u
from magic_generator import generator as generator
from magic_generator import CONFIG_FILE_PATH


def get_args() -> argparse.Namespace:
    """Get args from console."""

    config = c.get_config_file(CONFIG_FILE_PATH)
    parser = argparse.ArgumentParser(prog="magic_generator",
                                     description="Generate data based on schema",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--path", default=config['DEFAULT']['path'],
                        help="Path where all files should be saved")
    parser.add_argument("--count", type=int, default=config['DEFAULT']['count'],
                        help="Number of json files to generate."
                             "If --count=0 "
                             "then data will be written to the console.")
    parser.add_argument("--file", default=config['DEFAULT']['file'],
                        help="Base file_name. If there is no prefix, the final"
                             " file name will be file_name.jsonl. With prefix "
                             "full file name will be "
                             "file_name_file_prefix.jsonl")
    parser.add_argument("--prefix", default=config['DEFAULT']['prefix'],
                        choices=['count', 'random', 'uuid'],
                        help="What prefix for file name to use if more than 1"
                             " file needs to be generated.")
    parser.add_argument("--schema",
                        default=config['DEFAULT']['schema'],
                        help="It’s a string with json schema. "
                             "It could be loaded in two ways: "
                             "1) With path to json file with schema "
                             "2) with schema entered to command line.")
    parser.add_argument("--lines", type=int,
                        default=config['DEFAULT']['lines'],
                        help="Count of lines for each file.")
    parser.add_argument("--clear", action='store_true',
                        help="If this flag is on, before the script starts"
                             " creating new data files, all files in "
                             "path that match file name will be deleted.")
    parser.add_argument("--multiprocessing", type=int,
                        default=config['DEFAULT']['multiprocessing'],
                        help="The number of processes used to create files.")
    all_args = parser.parse_args()
    return all_args


def main(args: argparse.Namespace):

    files_count = args.count
    process_count = args.multiprocessing \
        if args.multiprocessing <= os.cpu_count() else os.cpu_count()
    lines_count = args.lines

    # Check if counts valid
    if files_count < 0:
        u.logger_error(4, 'Files', 0)
    if process_count < 1:
        u.logger_error(4, 'Process', 1)
    if lines_count < 1:
        u.logger_error(4, 'Lines', 1)

    output_path = u.get_output_dir_path(args.path)
    filename = args.file
    filepath = os.path.join(output_path, filename)
    file_prefix = u.get_prefix(args.prefix)

    data_schema = generator.get_data_schema(args.schema)

    if args.clear:
        u.clear_path(output_path, filename)

    # Generate and write data
    if files_count == 0:
        u.print_to_console(data_schema, lines_count)
    elif process_count == 1:
        u.write_data_to_files(files_count, filepath, file_prefix,
                              data_schema, lines_count)
    else:
        u.write_data_to_files_concurrently(process_count, files_count, filepath,
                                           file_prefix, data_schema, lines_count)

    logging.info('Exiting now.')


if __name__ == '__main__':

    input_args = get_args()
    main(input_args)
