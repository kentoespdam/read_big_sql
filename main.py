import argparse

from core.sql_dump_processor import SQLDumpProcessor


def main():
    """Main function dengan argument parsing lengkap"""
    parser = argparse.ArgumentParser(
        description="Tool untuk membaca, menganalisis, dan restore file SQL ke MariaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Contoh penggunaan:
      %(prog)s database_dump.sql analyze
      %(prog)s large_dump.sql extract --table users --output users.csv
      %(prog)s backup.sql restore --db-host localhost --db-user root --db-name mydb
      %(prog)s dump.sql restore --from-report analysis.json --tables users,products
            """
    )

    # Required arguments
    parser.add_argument('file', help='Path ke file SQL (support .gz compressed)')
    parser.add_argument('mode', choices=['analyze', 'extract', 'validate', 'split', 'restore'],
                        help='Mode operasi')

    # Database arguments (untuk mode restore)
    db_group = parser.add_argument_group('Database Options')
    db_group.add_argument('--db-host', help='MariaDB host address')
    db_group.add_argument('--db-port', type=int, default=3306, help='MariaDB port (default: 3306)')
    db_group.add_argument('--db-user', help='MariaDB username')
    db_group.add_argument('--db-password', help='MariaDB password')
    db_group.add_argument('--db-name', help='MariaDB database name')
    db_group.add_argument('--create-database', action='store_true',
                          help='Buat database jika tidak ada')
    db_group.add_argument('--drop-table', action='store_true',
                          help='Drop table jika sudah ada sebelum restore')
    db_group.add_argument('--batch-size', type=int, default=1000,
                          help='Ukuran batch untuk INSERT statements (default: 1000)')
    db_group.add_argument('--skip-errors', action='store_true',
                          help='Skip errors dan continue processing')

    # Restore from report options
    report_group = parser.add_argument_group('Restore from Report Options')
    report_group.add_argument('--from-report', action='store_true',
                              help='Restore dari file report JSON')
    report_group.add_argument('--tables', help='Daftar tabel untuk restore (comma-separated)')

    # Optional arguments umum
    parser.add_argument('--table', '-t', help='Nama tabel untuk mode extract')
    parser.add_argument('--output', '-o', help='File output')
    parser.add_argument('--encoding', '-e', default='utf-8',
                        help='Encoding file (default: utf-8)')
    parser.add_argument('--lines-per-file', '-l', type=int, default=100000,
                        help='Jumlah baris per file untuk mode split')
    parser.add_argument('--report', '-r', help='Simpan report lengkap ke file JSON')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Tampilkan informasi detail')

    args = parser.parse_args()

    # Validasi tambahan untuk mode restore
    if args.mode == 'restore':
        if not args.from_report:
            required_db_args = ['db_host', 'db_user', 'db_name']
            for arg in required_db_args:
                if not getattr(args, arg):
                    parser.error(f"Mode restore membutuhkan --{arg.replace('_', '-')}")
        else:
            if not args.report:
                parser.error("Mode restore dari report membutuhkan --report")

    processor = SQLDumpProcessor(args)
    processor.run()


if __name__ == "__main__":
    main()
