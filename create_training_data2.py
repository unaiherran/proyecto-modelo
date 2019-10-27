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


def calcular_de_imagenes_camara(fecha):
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
                  f"AND str_to_date('{sig_fecha_str}', '%Y-%m-%d %H:%i'));"

            df = pd.read_sql(sql, con=connection)
            df.to_csv('coches.csv')
            if df.empty:
                num_coches = 0
            else:
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
            if not df.empty:
                df2 = df.groupby('id').mean().mean()
                intensidad = df2['intensidad']
                ocupacion = df2['ocupacion']
                carga = df2['carga']

    return intensidad, ocupacion, carga


def calcular_de_eventos(cluster, fecha):

    fecha_ini = fecha - timedelta(minutes=120)
    fecha_fin = fecha + timedelta(minutes=60)
    format = '%Y-%m-%d %H:%M'

    eve_3h = 0
    eve_2h = 0
    eve_1h = 0
    eve_3h_g = 0
    eve_2h_g = 0
    eve_1h_g = 0

    fecha_ini_str = fecha_ini.strftime(format)
    fecha_fin_str = fecha_fin.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT * from DatosEventos eve inner join Cluster" \
                  f" clu on eve.cluster = clu.id_cluster where (eve.fecha BETWEEN " \
                  f"str_to_date('{fecha_ini_str}', '%Y-%m-%d %H:%i') AND " \
                  f"str_to_date('{fecha_fin_str}','%Y-%m-%d %H:%i')) AND " \
                  f"(clu.id_cluster = {cluster});"

            df = pd.read_sql(sql, con=connection)

            # df = pd.read_csv('dfe.csv')
            df['fecha'] = pd.to_datetime(df['fecha'])

            # Eventos 3h (-120 -> +60)
            eve_3h = df.count()['id']
            eve_3h_g = df[(df['gratuito']==1)].count()['id']

            # Eventos 2h (-60 -> +60)
            start_date = fecha - timedelta(minutes=60)
            end_date = fecha + timedelta(minutes=60)
            mask = (df['fecha'] > start_date) & (df['fecha'] <= end_date)
            df2 = df.loc[mask]
            eve_2h = df2.count()['id']
            eve_2h_g = df2[(df2['gratuito'] == 1)].count()['id']

            # Eventos 1h (-30 -> +30)
            start_date = fecha - timedelta(minutes=30)
            end_date = fecha + timedelta(minutes=30)
            mask = (df['fecha'] > start_date) & (df['fecha'] <= end_date)
            df2 = df.loc[mask]
            eve_1h = df2.count()['id']
            eve_1h_g = df2[(df2['gratuito'] == 1)].count()['id']

    return eve_3h, eve_3h_g, eve_2h, eve_2h_g, eve_1h, eve_1h_g


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


def insert_en_train_1_db(tb, fecha, cluster, num_coches=0, intensidad=0, ocupacion=0, carga=0,
                         dia_semana=0, dia_mes=0, festivo=False, eve_3h=0, eve_3h_g=0, eve_2h=0, eve_2h_g=0,
                         eve_1h=0, eve_1h_g=0):

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor()
            format = '%Y-%m-%d %H:%M'

            fecha_str = fecha.strftime(format)

            sql = f"SELECT * FROM {tb} WHERE fecha=str_to_date('{fecha_str}','%Y-%m-%d %H:%i') and cluster={cluster}"

            cur.execute(sql)
            data = cur.fetchall()

            if data:
                # hay que hacer update

                sql = f"update {tb} set num_coches={num_coches}, intensidad={intensidad}, ocupacion={ocupacion}, " \
                      f"carga={carga}, dia_semana={dia_semana}, dia_mes={dia_mes}, festivo={festivo}, " \
                      f"eve_3h={eve_3h}, eve_3h_g={eve_3h_g}, eve_2h={eve_2h}, eve_2h_g={eve_2h_g}, eve_1h={eve_1h}, " \
                      f"eve_1h_g={eve_3h} WHERE id_train={data[0][0]};"

            else:
                # hay que insertar
                sql = f"insert into {tb} (cluster, fecha, num_coches, intensidad, ocupacion, carga, dia_semana, " \
                      f"dia_mes, festivo, eve_3h, eve_3h_g, eve_2h, eve_2h_g, eve_1h, eve_1h_g ) " \
                      f"values ({cluster}, str_to_date('{fecha_str}','%Y-%m-%d %H:%i'), " \
                      f"{num_coches}, {intensidad},{ocupacion},{carga}, {dia_semana}, {dia_mes}, {festivo}, " \
                      f"{eve_3h}, {eve_3h_g}, {eve_2h}, {eve_2h_g}, {eve_1h}, {eve_1h_g});"

            cur.execute(sql)
            connection.commit()


def calculo_parametros_un_train(cluster, fecha, tb='train_1'):
    # Fecha es datetime
    # Cluster es int

    # calculo de db_imagenes_camara
    print(f'{datetime.now()} -> Realizando cálculos para cluster: {cluster}, fecha:{fecha}')
    print(datetime.now(), 'Calculando imagenes     ',end='\r')
    num_coches = calcular_de_imagenes_camara(fecha)

    # calculo de db_datos_trafico
    print(datetime.now(), ' Calculando datos trafico', end='\r')
    #intensidad, ocupacion, carga = calcular_de_datos_trafico(cluster, fecha)

    #calculo de db_festivos
    print(datetime.now(), 'Calculando festivos      ', end='\r')
    #dia_semana, dia_mes, festivo = calcular_de_fecha(fecha)

    # calculo de db_eventos
    print(datetime.now(), 'Calculando eventos       ', end='\r')
    #eve_3h, eve_3h_g, eve_2h, eve_2h_g, eve_1h, eve_1h_g = calcular_de_eventos(cluster, fecha)


    # escribir en bdd train_1
    print(datetime.now(), 'Escribiendo en dbb       \r', end='\r')

    # insert_en_train_1_db(tb, fecha, cluster, num_coches=num_coches, intensidad=intensidad, ocupacion=ocupacion,
    #                      carga=carga, dia_semana=dia_semana, dia_mes=dia_mes, festivo=festivo, eve_3h=eve_3h,
    #                      eve_3h_g=eve_3h_g, eve_2h=eve_2h, eve_2h_g=eve_2h_g, eve_1h=eve_1h, eve_1h_g=eve_1h_g)


def bucle(fecha_ini, fecha_fin, cluster_ini, cluster_fin, tb):
    fecha = fecha_ini
    while fecha <= fecha_fin:
        clu = cluster_ini
        while clu <= cluster_fin:
            calculo_parametros_un_train(clu, fecha, tb)
            clu += 1
        fecha = fecha + timedelta(minutes=15)


def main():
    fecha_ini = datetime.strptime("13-10-2019 00:00", "%d-%m-%Y %H:%M")
    fecha_fin = datetime.strptime("13-10-2019 00:15", "%d-%m-%Y %H:%M")
    clu_ini = 0
    clu_fin = 199
    tb = 'train_1'

    bucle(fecha_ini, fecha_fin,clu_ini,clu_fin, tb)


if __name__ == '__main__':
    main()