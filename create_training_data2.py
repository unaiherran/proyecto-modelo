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

def dataframe_vacio_de_cluster():
    cluster = list(range(200))
    empty_df = pd.DataFrame(list(zip(cluster)),
                            columns=['cluster'])
    return empty_df


def calcular_de_imagenes_camara(fecha):

    sig_fecha = fecha + timedelta(minutes=15)

    empty_df = dataframe_vacio_de_cluster()

    # preparar respuesta si no funciona la consulta
    cluster = list(range(200))
    lst0 = [0] * 200
    df3 = pd.DataFrame(list(zip(cluster, lst0)),
                       columns=['cluster', 'num_cars'])


    format = '%Y-%m-%d %H:%M'

    fecha_str =  fecha.strftime(format)
    sig_fecha_str = sig_fecha.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT num_cars, cam.id_camara, ima.fecha, cam.cluster from ImagenesCamarasTrafico ima " \
                  f"INNER JOIN CamarasTrafico cam ON ima.id_camara = cam.id_camara where " \
                  f"(ima.fecha BETWEEN str_to_date('{fecha_str}', '%Y-%m-%d %H:%i') " \
                  f"AND str_to_date('{sig_fecha_str}', '%Y-%m-%d %H:%i'));"

            df = pd.read_sql(sql, con=connection)

            if not df.empty:
                df = df.sort_values(by=['cluster'])
                df2 = df.groupby('cluster').mean()
                df2 = df2.drop(['id_camara'], axis=1)
                df2['cluster'] = list(df2.index.values)

                df3 = pd.merge(empty_df, df2, on='cluster', how='outer')
                df3 = df3.fillna(0)

    return df3


def calcular_de_datos_trafico(fecha):

    empty_df = dataframe_vacio_de_cluster()

    # preparar respuesta si no funciona la consulta
    cluster = list(range(200))
    lst0 = [0] * 200
    df3 = pd.DataFrame(list(zip(cluster, lst0, lst0, lst0)),
                       columns=['cluster', 'intensidad', 'ocupacion', 'carga'])
    sig_fecha = fecha + timedelta(minutes=15)

    format = '%Y-%m-%d %H:%M'

    fecha_str = fecha.strftime(format)
    sig_fecha_str = sig_fecha.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor()

            sql = f"SELECT sen.id , tra.intensidad, tra.ocupacion, tra.carga, tra.error, sen.cluster " \
                  f"from DatosTrafico tra INNER JOIN SensoresTrafico sen ON tra.id_sensor = sen.id " \
                  f"where (tra.fecha BETWEEN " \
                  f"str_to_date('{fecha_str}', '%Y-%m-%d %H:%i') AND " \
                  f"str_to_date('{sig_fecha_str}','%Y-%m-%d %H:%i')) AND " \
                  f"tra.error = 'N';"

            df = pd.read_sql(sql, con=connection)

            df = df.dropna(how='any',axis=0)

            if not df.empty:
                df = df.sort_values(by=['cluster'])
                df2 = df.groupby('cluster').mean()
                df2 = df2.drop(['id'], axis=1)
                df2['cluster'] = list(df2.index.values)
                df2 = df2.astype({"cluster": int})

                df3 = pd.merge(empty_df, df2, on='cluster', how='outer')
                df3 = df3.fillna(0)

    return df3


def calcular_de_eventos(fecha):

    fecha_ini = fecha - timedelta(minutes=120)
    fecha_fin = fecha + timedelta(minutes=60)
    format = '%Y-%m-%d %H:%M'

    # preparar respuesta si no funciona la consulta
    cluster = list(range(200))
    lst0 = [0] * 200
    df3 = pd.DataFrame(list(zip(cluster, lst0, lst0, lst0, lst0, lst0, lst0)),
                       columns=['cluster', 'eve_3h', 'eve_3h_g', 'eve_2h', 'eve_2h_g', 'eve_1h', 'eve_1h_g'])

    fecha_ini_str = fecha_ini.strftime(format)
    fecha_fin_str = fecha_fin.strftime(format)

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT * from DatosEventos eve " \
                  f"where (eve.fecha BETWEEN " \
                  f"str_to_date('{fecha_ini_str}', '%Y-%m-%d %H:%i') AND " \
                  f"str_to_date('{fecha_fin_str}','%Y-%m-%d %H:%i'));"

            print(sql)

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

    return df3


def calcular_de_fecha(fecha):
    dia_semana = fecha.weekday()
    dia = fecha.day
    festivo = 0

    if not LOCAL:
        if connection.is_connected():
            cur = connection.cursor();

            sql = f"SELECT * FROM Festivos where fecha=str_to_date('{fecha.year}-{fecha.month}-{fecha.day}'" \
                  f", '%Y-%m-%d');"

            cur.execute(sql)

            data = cur.fetchall()

            if data:
                festivo = 2

    cluster = list(range(200))
    lst_dia_semana = [dia_semana] * 200
    lst_dia_mes = [dia] * 200
    lst_festivo = [festivo] * 200

    df3 = pd.DataFrame(list(zip(cluster, lst_dia_semana, lst_dia_mes, lst_festivo)),
                       columns=['cluster', 'dia_semmana', 'dia_mes', 'festivo'])

    return df3


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


def calculo_parametros_un_train(fecha, tb='train_1'):
    # Fecha es datetime
    # Cluster es int
    print('----')
    print('Calculo de parametros un train')
    print(fecha)
    print(type(fecha))

    # calculo de db_imagenes_camara
    print(f'{datetime.now()} -> Realizando cálculos fecha:{fecha}')
    print(datetime.now(), 'Calculando imagenes     ',end='\r')

    df_coches = calcular_de_imagenes_camara(fecha)
    df_coches.to_csv('coches.csv')

    # calculo de db_datos_trafico
    print(datetime.now(), ' Calculando datos trafico', end='\r')
    df_trafico = calcular_de_datos_trafico(fecha)
    df_trafico.to_csv('trafico.csv')

    #calculo de db_festivos
    print(datetime.now(), 'Calculando festivos      ', end='\r')
    df_fecha = calcular_de_fecha(fecha)
    df_fecha.to_csv('fecha.csv')

    # calculo de db_eventos
    print(datetime.now(), 'Calculando eventos       ', end='\r')
    df_eventos = calcular_de_eventos(fecha)
    df_eventos.to_csv('eventos.csv')

    # escribir en bdd train_1
    print(datetime.now(), 'Escribiendo en dbb       \r', end='\r')

    # insert_en_train_1_db(tb, fecha, cluster, num_coches=num_coches, intensidad=intensidad, ocupacion=ocupacion,
    #                      carga=carga, dia_semana=dia_semana, dia_mes=dia_mes, festivo=festivo, eve_3h=eve_3h,
    #                      eve_3h_g=eve_3h_g, eve_2h=eve_2h, eve_2h_g=eve_2h_g, eve_1h=eve_1h, eve_1h_g=eve_1h_g)


def bucle(fecha_ini, fecha_fin, cluster_ini, cluster_fin, tb):
    fecha = fecha_ini
    while fecha <= fecha_fin:
        calculo_parametros_un_train(fecha, tb)

        fecha = fecha + timedelta(minutes=15)


def main():
    fecha_ini = datetime.strptime("25-10-2019 11:00", "%d-%m-%Y %H:%M")
    fecha_fin = datetime.strptime("25-10-2019 11:15", "%d-%m-%Y %H:%M")
    clu_ini = 30
    clu_fin = 31
    tb = 'train_1'

    bucle(fecha_ini, fecha_fin,clu_ini,clu_fin, tb)


if __name__ == '__main__':
    main()