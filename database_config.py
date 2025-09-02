#!/usr/bin/env python3
"""
Database configuration helper
"""

import configparser
import os
from typing import Dict, Any


def load_db_config(config_file: str = 'database.ini') -> Dict[str, Any]:
    """
    Load database configuration from INI file
    """
    config = configparser.ConfigParser()

    if not os.path.exists(config_file):
        return {}

    config.read(config_file)

    if 'mariadb' not in config:
        return {}

    return {
        'db_host': config['mariadb'].get('host', 'localhost'),
        'db_port': config['mariadb'].getint('port', 3306),
        'db_user': config['mariadb'].get('user', 'root'),
        'db_password': config['mariadb'].get('password', ''),
        'db_name': config['mariadb'].get('database', ''),
        'charset': config['mariadb'].get('charset', 'utf8mb4')
    }


def create_db_config(config_file: str = 'database.ini'):
    """
    Create sample database configuration file
    """
    config = configparser.ConfigParser()
    config['mariadb'] = {
        'host': 'localhost',
        'port': '3306',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database',
        'charset': 'utf8mb4'
    }

    with open(config_file, 'w') as f:
        config.write(f)

    print(f"✅ Sample configuration created: {config_file}")
    print("⚠️  Please edit with your actual database credentials!")


# Contoh file database.ini
"""
[mariadb]
host = localhost
port = 3306
user = root
password = your_password
database = your_database_name
charset = utf8mb4
"""