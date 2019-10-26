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
            num_coches = df.groupby('id_camara').mean().mean()['num_cars']

    return num_coches


def calcular_de_datos_trafico(cluster, fecha):
    intensidad = 0
    ocupacion = 0
    carga = 0

    sig_fecha = fecha + timedelta(minutes=15)

    format = '%Y-%m-%d %H:%M'

    fecha_str = fecha.strftime(format)
    sig_fecha_str = sig_fecha.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT sen.id , tra.fecha , tra.intensidad, tra.ocupacion, tra.carga, tra.error, clu.id_cluster " \
                  f"from DatosTrafico tra INNER JOIN SensoresTrafico sen ON tra.id_sensor = sen.id inner join Cluster" \
                  f" clu on sen.cluster = clu.id_cluster where (tra.fecha BETWEEN " \
                  f"str_to_date('{fecha_str}', '%Y-%m-%d %H:%i') AND " \
                  f"str_to_date('{sig_fecha_str}','%Y-%m-%d %H:%i')) AND " \
                  f"(clu.id_cluster = {cluster}) and tra.error = 'N';"

            df = pd.read_sql(sql, con=connection)

            df2 = df.groupby('id').mean().mean()
            intensidad = df2['intensidad']
            ocupacion = df2['ocupacion']
            carga = df2['carga']

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


def insert_en_train_1_db(fecha, cluster, num_coches=0, intensidad=0, ocupacion=0, carga=0,
                         dia_semana=0, dia_mes=0, festivo=False):
    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();
            format = '%Y-%m-%d %H:%M'

            fecha_str = fecha.strftime(format)

            sql = f"SELECT * FROM train_1 WHERE fecha=str_to_date('{fecha_str}','%Y-%m-%d %H:%i') and cluster={cluster}"

            print(sql)

            cur.execute(sql)

            data = cur.fetchall()

            print(data)

            if data:
                # hay que hacer update
                pass
            else:
                # hay que insertar
                sql = f"insert into train_1 (cluster, fecha, num_coches, intensidad, ocupacion, carga, dia_semana, " \
                      f"dia_mes, festivo) values ({cluster}, str_to_date('{fecha_str}','%Y-%m-%d %H:%i'), {num_coches}," \
                      f"{intensidad},{ocupacion},{carga}, {dia_semana}, {dia_mes}, {festivo};"

                pass



def calculo_parametros_un_train(cluster, fecha):
    # Fecha es datetime
    # Cluster es int

    print(datetime.now())
    # calculo de db_imagenes_camara
    num_coches = calcular_de_imagenes_camara(cluster, fecha)

    print(datetime.now())
    # calculo de db_datos_trafico
    intensidad, ocupacion, carga = calcular_de_datos_trafico(cluster, fecha)

    print(datetime.now())
    #calculo de db_festivos
    dia_semana, dia_mes, festivo = calcular_de_fecha(fecha)

    print(datetime.now())
    # calculo de db_eventos

    print(datetime.now())
    # escribir en bdd train_1
    insert_en_train_1_db(fecha, cluster, num_coches=num_coches, intensidad=intensidad, ocupacion=ocupacion,
                         carga=carga, dia_semana=dia_semana, dia_mes=dia_mes, festivo=festivo)
    print(datetime.now())

def main():
    fecha = datetime.strptime("22-10-2019 12:00", "%d-%m-%Y %H:%M")
    cluster = 1
    calculo_parametros_un_train(cluster, fecha)


if __name__ == '__main__':
    main()