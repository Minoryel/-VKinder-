import psycopg2
from config import *

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

connection.autocommit = True

def create_table_users():
    """создание таблицы users"""
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS users(
            id serial,
            first_name varchar(50) NOT NULL,
            last_name varchar(25) NOT NULL,
            id_vk varchar(20) NOT NULL PRIMARY KEY,
            link_vk varchar(50));"""
        )
    print("[INFO] Table users was created.")

    def create_table_viewed_users():
        """создание таблицы viewed_users"""
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS viewed_users(
                id serial,
                id_vk varchar(50) PRIMARY KEY);"""
            )
        print("[INFO] Table viewed_users was created.")

    def insert_data_users(first_name, last_name, id_vk, link_vk):
        """вставка данных в таблицу users"""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""INSERT INTO users (first_name, last_name. id_vk, link_vk)
            VALUES ('{first_name}', '{last_name}', '{id_vk}', '{link_vk}');"""
        )

    def insert_data_viewed_users(id_vk, offset):
        """вставка данных в таблицу viewed_users"""
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO viewed_users (vk_id) 
                VALUES ('{id_vk}')
                OFFSET '{offset}';"""
            )

    def select(offset):
        """выборка из непросмотренных"""
        with connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT u.first_name,
                            u.last_name,
                            u.id_vk,
                            u.link_vk,
                            su.id_vk
                            FROM users AS u
                            LEFT JOIN viewed_users AS su 
                            ON u.id_vk = su.id_vk
                            WHERE su.id_vk IS NULL
                            OFFSET '{offset}';"""
            )
            return cursor.fetchone()

    def drop_users():
        """удаление таблицы users каскадом"""
        with connection.cursor() as cursor:
            cursor.execute(
                """DROP TABLE IF EXISTS users CASCADE;"""
            )
            print('[INFO] Table USERS was deleted.')

    def drop_viewed_users():
        """Уудаление таблицы viewed_users каскадом"""
        with connection.cursor() as cursor:
            cursor.execute(
                """DROP TABLE  IF EXISTS viewed_users CASCADE;"""
            )
            print('[INFO] Table VIEWED_USERS was deleted.')

    def creating_database():
        drop_users()
        drop_viewed_users()
        create_table_users()
        create_table_viewed_users()