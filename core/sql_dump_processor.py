#!/usr/bin/env python3
"""
SQL Large File Reader - Tool untuk membaca dan menganalisis file SQL yang sangat besar
"""

import csv
import gzip
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime

import mysql
from mysql.connector import Error
from tqdm import tqdm

from core.extract import extract_table_name
from core.parse import parse_values

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SQLDumpProcessor:
    def __init__(self, args):
        self.args = args
        self.stats = {
            'file_info': {},
            'statements': {
                'create_table': 0,
                'insert': 0,
                'alter': 0,
                'drop': 0,
                'other': 0,
                'total': 0
            },
            'tables': {},
            'line_count': 0,
            'start_time': 0.0,
            'end_time': 0.0
        }
        self.db_connection = None

    def run(self):
        """Main execution method"""
        try:
            self._validate_args()
            self._print_header()

            self.stats['start_time'] = time.time()

            if self.args.mode == 'analyze':
                self._analyze_dump()
            elif self.args.mode == 'extract':
                self._extract_data()
            elif self.args.mode == 'validate':
                self._validate_dump()
            elif self.args.mode == 'split':
                self._split_dump()
            elif self.args.mode == 'restore':
                self._restore_to_db()

            self.stats['end_time'] = time.time()
            self._generate_report()

        except Exception as e:
            logger.error(f"Error: {str(e)}")
            if self.args.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)
        finally:
            self._close_db_connection()

    def _validate_args(self):
        """Validate command line arguments"""
        if self.args.mode != 'restore' and not os.path.exists(self.args.file):
            raise FileNotFoundError(f"File '{self.args.file}' tidak ditemukan")

        if self.args.mode != 'restore' and not os.path.isfile(self.args.file):
            raise ValueError(f"'{self.args.file}' bukan sebuah file")

        # Validasi argumen database untuk mode restore
        if self.args.mode == 'restore':
            required_db_args = ['db_host', 'db_user', 'db_name']
            for arg in required_db_args:
                if not getattr(self.args, arg):
                    raise ValueError(f"Argument database required untuk mode restore")

    def _print_header(self):
        """Print program header"""
        print("=" * 80)
        print("🔍 SQL Large File Processor dengan Restore MariaDB")
        print("=" * 80)
        print(f"📁 File: {self.args.file}")
        print(f"🎯 Mode: {self.args.mode}")
        if self.args.mode == 'restore':
            print(f"🗄️  Database: {self.args.db_name}@{self.args.db_host}")
        print("=" * 80)
        print()

    def _get_db_connection(self):
        """Establish MariaDB connection"""
        if self.db_connection and self.db_connection.is_connected():
            return self.db_connection

        try:
            connection_config = {
                'host': self.args.db_host,
                'user': self.args.db_user,
                'password': self.args.db_password,
                'database': self.args.db_name,
                'port': self.args.db_port,
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_general_ci',
                'autocommit': True
            }

            # Remove None values
            connection_config = {k: v for k, v in connection_config.items() if v is not None}

            logger.info(f"Connecting to database {self.args.db_name} on {self.args.db_host}")
            self.db_connection = mysql.connector.connect(**connection_config)

            if self.db_connection.is_connected():
                db_info = self.db_connection.get_server_info()
                logger.info(f"Connected to MariaDB server version {db_info}")
                return self.db_connection

        except Error as e:
            logger.error(f"Error connecting to MariaDB: {e}")
            raise

    def _close_db_connection(self):
        """Close database connection"""
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("Database connection closed")

    def _execute_sql(self, sql, params=None):
        """Execute SQL statement"""
        connection = self._get_db_connection()
        cursor = connection.cursor()

        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            if cursor.description:  # If it's a SELECT query
                result = cursor.fetchall()
                return result
            else:
                return cursor.rowcount

        except Error as e:
            logger.error(f"SQL Error: {e}")
            logger.error(f"Failed SQL: {sql}")
            raise
        finally:
            cursor.close()

    def _restore_to_db(self):
        """Restore data dari file SQL ke MariaDB"""
        file_path = self._decompress_if_needed()

        logger.info(f"Starting restore from {file_path} to {self.args.db_name}")

        # Check if database exists, create if not
        if not self._database_exists():
            if self.args.create_database:
                self._create_database()
            else:
                raise ValueError(
                    f"Database {self.args.db_name} tidak ditemukan. Gunakan --create-database untuk membuat otomatis.")

        # Process file dan execute SQL
        current_statement = ""
        in_multiline_comment = False
        in_string = False
        escape_next = False
        statements_executed = 0

        with open(file_path, 'r', encoding=self.args.encoding) as file:
            for line in tqdm(file, desc="Restoring to DB", unit="lines"):
                processed = self._process_line(line, in_multiline_comment, in_string, escape_next)
                current_statement += processed['content'] + " "

                in_multiline_comment = processed['in_multiline_comment']
                in_string = processed['in_string']
                escape_next = processed['escape_next']

                # Execute complete statement
                if ';' in processed['content'] and not in_string:
                    if current_statement.strip() and not current_statement.strip().startswith('--'):
                        try:
                            self._execute_sql(current_statement.strip())
                            statements_executed += 1

                            if self.args.verbose and statements_executed % 100 == 0:
                                logger.info(f"Executed {statements_executed} statements")

                        except Exception as e:
                            if self.args.skip_errors:
                                logger.warning(f"Skipping error in statement: {e}")
                            else:
                                raise

                    current_statement = ""

        # Execute any remaining statement
        if current_statement.strip() and not current_statement.strip().startswith('--'):
            self._execute_sql(current_statement.strip())
            statements_executed += 1

        logger.info(f"✅ Restore complete! {statements_executed} statements executed")
        self._cleanup_temp_file(file_path)

    def _database_exists(self):
        """Check if database exists"""
        try:
            # Temporary connection without database
            config = {
                'host': self.args.db_host,
                'user': self.args.db_user,
                'password': self.args.db_password,
                'port': self.args.db_port,
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_general_ci'
            }

            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
            cursor.close()
            connection.close()

            return self.args.db_name in databases

        except Error as e:
            logger.error(f"Error checking database existence: {e}")
            return False

    def _create_database(self):
        """Create database"""
        try:
            config = {
                'host': self.args.db_host,
                'user': self.args.db_user,
                'password': self.args.db_password,
                'port': self.args.db_port,
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_general_ci'
            }

            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE {self.args.db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
            cursor.close()
            connection.close()

            logger.info(f"✅ Database {self.args.db_name} created successfully")

        except Error as e:
            logger.error(f"Error creating database: {e}")
            raise

    def _restore_from_report(self):
        """Restore specific tables dari report JSON"""
        if not self.args.report:
            raise ValueError("File report diperlukan untuk restore dari report")

        if not os.path.exists(self.args.report):
            raise FileNotFoundError(f"Report file '{self.args.report}' tidak ditemukan")

        # Load report data
        with open(self.args.report, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        tables_to_restore = self.args.tables.split(',') if self.args.tables else list(
            report_data.get('tables', {}).keys())

        logger.info(f"Restoring tables: {', '.join(tables_to_restore)}")

        file_path = self._decompress_if_needed()

        for table_name in tables_to_restore:
            self._restore_single_table(file_path, table_name)

        self._cleanup_temp_file(file_path)

    def _restore_single_table(self, file_path, table_name):
        """Restore single table dari dump file"""
        logger.info(f"🔄 Restoring table: {table_name}")

        # Extract CREATE TABLE statement
        create_table_stmt = self._extract_create_table(file_path, table_name)
        if create_table_stmt:
            try:
                self._execute_sql(create_table_stmt)
                logger.info(f"✅ Table {table_name} created successfully")
            except Error as e:
                if "already exists" in str(e).lower() and self.args.drop_table:
                    logger.info(f"🔄 Table {table_name} already exists, dropping...")
                    self._execute_sql(f"DROP TABLE IF EXISTS {table_name}")
                    self._execute_sql(create_table_stmt)
                    logger.info(f"✅ Table {table_name} recreated successfully")
                else:
                    raise

        # Extract and execute INSERT statements
        insert_count = self._extract_and_execute_inserts(file_path, table_name)
        logger.info(f"✅ Inserted {insert_count} rows into {table_name}")

    def _extract_create_table(self, file_path, table_name):
        """Extract CREATE TABLE statement untuk table tertentu"""
        create_pattern = re.compile(
            rf'(CREATE TABLE (?:IF NOT EXISTS )?`?{re.escape(table_name)}`?.*?;)',
            re.IGNORECASE | re.DOTALL
        )

        with open(file_path, 'r', encoding=self.args.encoding) as file:
            content = file.read()
            match = create_pattern.search(content)
            if match:
                return match.group(1)

        logger.warning(f"CREATE TABLE statement not found for {table_name}")
        return None

    def _extract_and_execute_inserts(self, file_path, table_name):
        """Extract dan execute INSERT statements untuk table tertentu"""
        insert_pattern = re.compile(
            rf'(INSERT INTO `?{re.escape(table_name)}`?.*?;)',
            re.IGNORECASE | re.DOTALL
        )

        insert_count = 0
        batch_size = self.args.batch_size
        current_batch = []

        with open(file_path, 'r', encoding=self.args.encoding) as file:
            content = file.read()
            matches = insert_pattern.findall(content)

            for i, insert_stmt in enumerate(tqdm(matches, desc=f"Inserting into {table_name}")):
                current_batch.append(insert_stmt)

                if len(current_batch) >= batch_size:
                    self._execute_batch(current_batch)
                    insert_count += len(current_batch)
                    current_batch = []

                if self.args.verbose and i % 1000 == 0:
                    logger.info(f"Processed {i} INSERT statements for {table_name}")

            # Execute remaining batch
            if current_batch:
                self._execute_batch(current_batch)
                insert_count += len(current_batch)

        return insert_count

    def _execute_batch(self, statements):
        """Execute batch of SQL statements"""
        connection = self._get_db_connection()
        cursor = connection.cursor()

        try:
            for statement in statements:
                cursor.execute(statement)
        except Error as e:
            if self.args.skip_errors:
                logger.warning(f"Skipping error in batch: {e}")
            else:
                raise
        finally:
            cursor.close()

    def _process_line(self, line, in_comment, in_string, escape_next):
        """Process a line handling comments and strings"""
        result = {
            'content': '',
            'in_multiline_comment': in_comment,
            'in_string': in_string,
            'escape_next': escape_next
        }

        i = 0
        content_buffer = []
        line = line.rstrip()

        while i < len(line):
            char = line[i]

            if escape_next:
                content_buffer.append(char)
                escape_next = False
                i += 1
                continue

            if in_string:
                content_buffer.append(char)
                if char == '\\':
                    escape_next = True
                elif char == in_string:
                    in_string = False
                i += 1
                continue

            if in_comment:
                if char == '*' and i + 1 < len(line) and line[i + 1] == '/':
                    in_comment = False
                    i += 2
                else:
                    i += 1
                continue

            if char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                break

            if char == '/' and i + 1 < len(line) and line[i + 1] == '*':
                in_comment = True
                i += 2
                continue

            if char in ['\'', '"', '`']:
                in_string = char
                content_buffer.append(char)
                i += 1
                continue

            content_buffer.append(char)
            i += 1

        result['content'] = ''.join(content_buffer)
        result['in_multiline_comment'] = in_comment
        result['in_string'] = in_string
        result['escape_next'] = escape_next

        return result

    def _analyze_dump(self):
        """Analyze SQL dump file"""
        file_path = self._decompress_if_needed()

        file_size = os.path.getsize(file_path)
        self.stats['file_info'] = {
            'path': file_path,
            'size_bytes': file_size,
            'size_mb': file_size / (1024 * 1024),
            'size_gb': file_size / (1024 * 1024 * 1024),
            'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
        }

        print(f"📊 Analyzing file: {file_path}")
        print(f"💾 Size: {self.stats['file_info']['size_mb']:.2f} MB")

        with open(file_path, 'r', encoding=self.args.encoding) as file:
            for line in tqdm(file, desc="Analyzing SQL", unit="lines"):
                self.stats['line_count'] += 1
                self._process_line_for_analysis(line)

        self._cleanup_temp_file(file_path)

    def _process_line_for_analysis(self, line):
        """Process a line for analysis"""
        line_upper = line.upper().strip()

        if not line_upper or line_upper.startswith('--'):
            return

        self.stats['statements']['total'] += 1

        if 'CREATE TABLE' in line_upper:
            self.stats['statements']['create_table'] += 1
            table_name = extract_table_name(line)
            if table_name:
                self.stats['tables'][table_name] = self.stats['tables'].get(table_name, 0) + 1

        elif 'INSERT INTO' in line_upper:
            self.stats['statements']['insert'] += 1
            table_name = extract_table_name(line)
            if table_name:
                self.stats['tables'][table_name] = self.stats['tables'].get(table_name, 0) + 1

        elif 'ALTER TABLE' in line_upper:
            self.stats['statements']['alter'] += 1

        elif 'DROP TABLE' in line_upper:
            self.stats['statements']['drop'] += 1

        else:
            self.stats['statements']['other'] += 1

    def _extract_data(self):
        """Extract data from SQL dump"""
        if not self.args.table:
            raise ValueError("Table name required for extract mode. Use --table option.")

        file_path = self._decompress_if_needed()
        output_file = self.args.output or f"{self.args.table}_extracted.csv"

        print(f"📥 Extracting data from table: {self.args.table}")
        print(f"💾 Output: {output_file}")

        self._extract_table_data(file_path, self.args.table, output_file)
        self._cleanup_temp_file(file_path)

    def _extract_table_data(self, file_path, table_name, output_file):
        """Extract specific table data to CSV"""
        insert_pattern = re.compile(
            rf'INSERT INTO `?{re.escape(table_name)}`?\s*\(([^)]+)\)\s*VALUES\s*(.*?)(?=\);|$)',
            re.IGNORECASE | re.DOTALL
        )

        csv_file = None
        csv_writer = None
        columns = None
        extracted_rows = 0

        with open(file_path, 'r', encoding=self.args.encoding) as sql_file:
            for line in tqdm(sql_file, desc=f"Extracting {table_name}", unit="lines"):
                if f'INSERT INTO' in line.upper() and table_name in line:
                    matches = insert_pattern.findall(line)
                    for cols, values in matches:
                        if not columns:
                            columns = [col.strip().strip('`"') for col in cols.split(',')]
                            csv_file = open(output_file, 'w', newline='', encoding='utf-8')
                            csv_writer = csv.DictWriter(csv_file, fieldnames=columns)
                            csv_writer.writeheader()
                            print(f"📋 Columns: {', '.join(columns)}")

                        # Simple value parsing
                        values_list = parse_values(values)
                        if csv_writer and len(values_list) == len(columns):
                            csv_writer.writerow(dict(zip(columns, values_list)))
                            extracted_rows += 1

        if csv_file:
            csv_file.close()

        print(f"✅ Extracted {extracted_rows} rows from table '{table_name}'")

    def _validate_dump(self):
        """Validate SQL dump integrity"""
        file_path = self._decompress_if_needed()

        print("🔍 Validating SQL dump integrity...")

        errors = []
        tables_created = set()
        tables_referenced = set()

        with open(file_path, 'r', encoding=self.args.encoding) as file:
            for i, line in enumerate(tqdm(file, desc="Validating", unit="lines")):
                line_upper = line.upper()

                if 'CREATE TABLE' in line_upper:
                    table_name = extract_table_name(line)
                    if table_name:
                        tables_created.add(table_name)

                elif 'INSERT INTO' in line_upper:
                    table_name = extract_table_name(line)
                    if table_name:
                        tables_referenced.add(table_name)

        missing_tables = tables_referenced - tables_created
        for table in missing_tables:
            errors.append(f"Missing CREATE TABLE for: {table}")

        print(f"✅ Validation complete. Found {len(errors)} issues.")
        for error in errors:
            print(f"   ⚠️  {error}")

        self._cleanup_temp_file(file_path)

    def _split_dump(self):
        """Split large SQL dump into smaller files"""
        file_path = self._decompress_if_needed()
        output_dir = self.args.output or "split_output"

        os.makedirs(output_dir, exist_ok=True)

        print(f"✂️  Splitting file into {self.args.lines_per_file} lines per chunk...")
        print(f"📁 Output directory: {output_dir}")

        file_count = 0
        line_count = 0
        current_file = None

        with open(file_path, 'r', encoding=self.args.encoding) as main_file:
            for line in tqdm(main_file, desc="Splitting", unit="lines"):
                if line_count % self.args.lines_per_file == 0:
                    if current_file:
                        current_file.close()
                    file_count += 1
                    output_path = os.path.join(output_dir, f"part_{file_count:04d}.sql")
                    current_file = open(output_path, 'w', encoding='utf-8')
                    if self.args.verbose:
                        print(f"📄 Creating part {file_count}: {output_path}")

                current_file.write(line)
                line_count += 1

        if current_file:
            current_file.close()

        print(f"✅ Split complete! Created {file_count} files in '{output_dir}'")
        self._cleanup_temp_file(file_path)

    def _decompress_if_needed(self):
        """Handle compressed files"""
        if self.args.file.endswith('.gz'):
            if self.args.verbose:
                print("🔓 Decompressing gzip file...")

            decompressed_path = self.args.file[:-3]
            with gzip.open(self.args.file, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)  # noqa

            return decompressed_path
        return self.args.file

    def _cleanup_temp_file(self, file_path):
        """Clean up temporary decompressed file"""
        if file_path != self.args.file and os.path.exists(file_path):
            if self.args.verbose:
                print(f"🗑️  Removing temporary file: {file_path}")
            os.remove(file_path)

    def _generate_report(self):
        """Generate and print analysis report"""
        execution_time = self.stats['end_time'] - self.stats['start_time']

        print("\n" + "=" * 70)
        print("📊 ANALYSIS REPORT")
        print("=" * 70)

        print(f"⏱️  Execution time: {execution_time:.2f} seconds")
        print(f"📄 Total lines processed: {self.stats['line_count']:,}")

        if self.stats['file_info']:
            print(f"💾 File size: {self.stats['file_info']['size_mb']:.2f} MB")

        print(f"\n📋 Statement statistics:")
        for stmt_type, count in self.stats['statements'].items():
            print(f"   • {stmt_type.replace('_', ' ').title():<15}: {count:,}")

        if self.stats['tables']:
            print(f"\n📊 Tables found ({len(self.stats['tables'])}):")
            for table, count in sorted(self.stats['tables'].items()):
                print(f"   • {table}: {count} statements")

        # Save report to file if requested
        if self.args.report:
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'arguments': vars(self.args),
                'statistics': self.stats,
                'execution_time_seconds': execution_time
            }

            with open(self.args.report, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

            print(f"\n📝 Full report saved to: {self.args.report}")

        print("=" * 70)
