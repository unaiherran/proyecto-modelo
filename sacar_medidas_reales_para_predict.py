from secret import *
import mysql.connector

from datetime import datetime
from datetime import timedelta
from time import sleep

def main():
    connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_passwd,
        database=db_database,
        port=db_port)

    while True:
        ahora = datetime.now()
        if connection.is_connected():
            cur = connection.cursor()
            hora_maxima_consulta = ahora - timedelta(minutes=60)
            sql = f"SELECT * FROM proyecto.predict where fecha < '{hora_maxima_consulta}' and ocu_medida is null limit 1;"

            cur.execute(sql)

            consulta = cur.fetchall()
            if consulta:
                fecha_a_buscar = consulta[0]
                print(fecha_a_buscar)
                print(consulta)

            sleep(60)

if __name__ == '__main__':
    main()