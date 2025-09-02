import re


def extract_table_name(line):
    """Extract table name from SQL line"""
    patterns = [
        r'CREATE TABLE (?:IF NOT EXISTS )?[`"]?(\w+)[`"]?',
        r'INSERT INTO [`"]?(\w+)[`"]?',
        r'INSERT IGNORE INTO [`"]?(\w+)[`"]?'
    ]

    for pattern in patterns:
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            return match.group(1)
    return None
