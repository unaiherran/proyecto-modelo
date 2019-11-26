from secret import *
import mysql.connector


def main():
    connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_passwd,
        database=db_database,
        port=db_port)

    while True:


if __name__ == '__main__':
    main()