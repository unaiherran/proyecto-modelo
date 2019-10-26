from datetime import datetime
from datetime import timedelta

import mysql.connector

import pandas as pd

from secret import *

DEBUG = True
LOCAL = False

# Conectarse a la base de datos

if not LOCAL:
    connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_passwd,
        database=db_database,
        port=db_port
    )


def calcular_de_imagenes_camara(cluster, fecha):
    num_coches = 0
    return num_coches


def calcular_de_datos_trafico(cluster, fecha):
    intensidad = 0
    ocupacion = 0
    carga = 0

    sig_fecha = fecha + timedelta(minutes=15)

    format = '%Y-%m-%d %H:%M'

    fecha_str =  fecha.strftime(format)
    sig_fecha_str = sig_fecha.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT num_cars, clu.id_cluster , cam.id_camara, ima.fecha from ImagenesCamarasTrafico ima " \
                  f"INNER JOIN CamarasTrafico cam ON ima.id_camara = cam.id_camara inner join " \
                  f"Cluster clu on cam.Cluster = clu.id_cluster where " \
                  f"(ima.fecha BETWEEN str_to_date('{fecha_str}', '%Y-%m-%d %H:%i') " \
                  f"AND str_to_date('{sig_fecha_str}', '%Y-%m-%d %H:%i')) and (clu.id_cluster = {cluster});"

            df = pd.read_sql(sql, con=connection)

            print(df)

    return intensidad, ocupacion, carga


def calcular_de_eventos(cluster, fecha):
    return True


def calcular_de_fecha(fecha):
    dia_semana = fecha.weekday()
    dia = fecha.day
    festivo = False

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT * FROM Festivos where fecha=str_to_date('{fecha.year}-{fecha.month}-{fecha.day}'" \
                  f", '%Y-%m-%d');"

            cur.execute(sql)

            data = cur.fetchall()

            if data:
                festivo = True

    return dia_semana, dia, festivo


def poblar_train(cluster, fecha):
    # Fecha es dattime
    # Cluster es int
    print (fecha, cluster)

    # calculo de db_imagenes_camara
    num_coches = calcular_de_imagenes_camara(cluster, fecha)

    # calculo de db_datos_trafico
    intensidad, ocupacion, carga = calcular_de_datos_trafico(cluster, fecha)

    #calculo de db_festivos
    dia_semana, dia_mes, festivo = calcular_de_fecha(fecha)
    print(dia_semana, dia_mes, festivo)

    # calculo de db_eventos

    pass
# calculo de parametros

# q a  datos trafico
# calculo de parametros

# q a festivos

# escribir en bdd train_1


def main():
    fecha = datetime.strptime("22-10-2019 12:05", "%d-%m-%Y %H:%M")
    cluster = 1
    poblar_train(cluster, fecha)


if __name__ == '__main__':
    main()