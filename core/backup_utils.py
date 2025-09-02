#!/usr/bin/env python3
"""
Utilities untuk backup dan restore database
"""

import subprocess
import os
from datetime import datetime


class MariaDBBackup:
    @staticmethod
    def create_backup(host, user, password, database, output_file=None):
        """
        Create backup menggunakan mysqldump
        """
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{database}_backup_{timestamp}.sql"

        cmd = [
            'mysqldump',
            f'--host={host}',
            f'--user={user}',
            f'--password={password}',
            '--single-transaction',
            '--routines',
            '--triggers',
            '--events',
            database
        ]

        try:
            with open(output_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)

            if result.returncode == 0:
                print(f"✅ Backup created: {output_file}")
                return output_file
            else:
                print(f"❌ Backup failed: {result.stderr}")
                return None

        except Exception as e:
            print(f"❌ Backup error: {e}")
            return None

    @staticmethod
    def compress_backup(sql_file, remove_original=True):
        """
        Compress backup file
        """
        compressed_file = f"{sql_file}.gz"

        try:
            import gzip
            import shutil

            with open(sql_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            if remove_original:
                os.remove(sql_file)

            print(f"✅ Backup compressed: {compressed_file}")
            return compressed_file

        except Exception as e:
            print(f"❌ Compression error: {e}")
            return None