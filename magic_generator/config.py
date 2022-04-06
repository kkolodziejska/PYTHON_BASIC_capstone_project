import configparser


def get_config_file(configfile: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(configfile)
    return config
