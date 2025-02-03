import os
import subprocess

import psycopg2
from psycopg2 import sql
from colorama import Fore, Style, init

from settings.config import db_settings
from app.common.log_result import logger
from app.common.log_timer import log_timer


init(autoreset=True)
CURRENT_DIR: str = os.path.dirname(__file__)
PG_RESTORE_PATH: str = os.path.join(
    'E:\\', 'PostgreSQL', '10', 'bin', 'pg_restore.exe'
)


def create_database(db_name: str) -> bool:
    try:
        connection = psycopg2.connect(
            dbname='postgres',
            user=db_settings.DB_USER,
            password=db_settings.DB_PSWD,
            host=db_settings.DB_HOST,
            port=db_settings.DB_PORT
        )
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), [db_name]
        )
        exists = cursor.fetchone() is not None

        if not exists:
            cursor.execute(
                sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name))
            )
            return True
        else:
            raise ValueError(f'База данных {db_name} уже существует.')
    except Exception as e:
        print(f'Ошибка при создании базы данных: {e}')
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def restore_database(
    pg_restore_path: str, dump_file: str, db_name: str,
    user: str, password: str, host: str, port: str | int
):
    """Восстанавливам базу данных из дампа."""
    try:
        os.environ['PGPASSWORD'] = password
        command = [
            pg_restore_path,
            '-U', user,
            '-h', host,
            '-p', str(port),
            '-d', db_name, dump_file
        ]
        subprocess.run(command, check=True)
        logger.info(f'База данных {db_name} добавлена из файла {dump_file}')
        print(
            Fore.BLUE + Style.DIM + 'База данных ' +
            Fore.WHITE + Style.BRIGHT + db_name +
            Fore.BLUE + Style.DIM + ' успешно восстановлена из файла ' +
            Fore.WHITE + Style.BRIGHT + dump_file
        )
    except subprocess.CalledProcessError as e:
        print(f'Ошибка при восстановлении базы данных: {e}')
    finally:
        del os.environ['PGPASSWORD']


@log_timer('create_dump_db')
def main():
    db_name: str = 'dump_db'
    dump_file: str = os.path.join(
        CURRENT_DIR,
        'database', 'dumps', 'tech_pris', '2025-01-17__tech_pris.dump'
    )
    if create_database(db_name):
        restore_database(
            PG_RESTORE_PATH,
            dump_file, db_name,
            db_settings.DB_USER, db_settings.DB_PSWD,
            db_settings.DB_HOST, db_settings.DB_PORT
        )


if __name__ == '__main__':
    main()
