import logging
import os


ACCEPTED_VALUES = ['timestamp', 'str', 'int']

CONFIG_FILE_PATH = os.path.join('magic_generator', 'default.ini')

(FILE_ERROR,
 JSON_ERROR,
 VALUE_ERROR,
 TYPE_ERROR,
 COUNT_ERROR,
 DIR_ERROR,
 FORMAT_ERROR,
 REMOVE_ERROR,
 WRITE_ERROR
 ) = range(9)

ERRORS = {FILE_ERROR: 'File {} does not exist. Exiting now.',
          JSON_ERROR: '{} is not a valid json format. Exiting now.',
          VALUE_ERROR: '{} is not a valid value for {} type. Exiting now.',
          TYPE_ERROR: '{} is not a valid type. '
                      'Parser supports only timestamp, str, int. Exiting now.',
          COUNT_ERROR: '{} count cannot be less than {}. Exiting now.',
          DIR_ERROR: 'Path {} is not a directory. Exiting now.',
          FORMAT_ERROR: '{}: {} is not a valid schema format. Exiting now.',
          REMOVE_ERROR: 'Removing file {} failed. Exiting now.',
          WRITE_ERROR: 'Writing file {} failed. Exiting now.'
          }


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
