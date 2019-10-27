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

            sql = f"SELECT fecha, gratuito, cluster from DatosEventos eve " \
                  f"where (eve.fecha BETWEEN " \
                  f"str_to_date('{fecha_ini_str}', '%Y-%m-%d %H:%i') AND " \
                  f"str_to_date('{fecha_fin_str}','%Y-%m-%d %H:%i'));"

            print(sql)

            df = pd.read_sql(sql, con=connection)

            df['evento'] = 1
            df['fecha'] = pd.to_datetime(df['fecha'])
            df = df.sort_values(by=['cluster'])

            # Eventos 3h (-120 -> +60)
            df3h = df.groupby('cluster').sum()
            df3h.columns = ['eve_3h_g', 'eve_3h']
            df3h['cluster'] = list(df3h.index.values)

            # Eventos 2h (-60 -> +60)
            start_date = fecha - timedelta(minutes=60)
            end_date = fecha + timedelta(minutes=60)
            mask = (df['fecha'] > start_date) & (df['fecha'] <= end_date)
            df2 = df.loc[mask]
            df2h = df2.groupby('cluster').sum()
            df2h.columns = ['eve_2h_g', 'eve_2h']
            df2h['cluster'] = list(df2h.index.values)

            # Eventos 1h (-30 -> +30)
            start_date = fecha - timedelta(minutes=30)
            end_date = fecha + timedelta(minutes=30)
            mask = (df['fecha'] > start_date) & (df['fecha'] <= end_date)
            df1 = df.loc[mask]
            df1h = df1.groupby('cluster').sum()
            df1h.columns = ['eve_1h_g', 'eve_1h']
            df1h['cluster'] = list(df1h.index.values)

            # merge de todos los df
            df3 = dataframe_vacio_de_cluster()

            df3 = pd.merge(df3, df3h, on='cluster', how='outer')
            df3 = pd.merge(df3, df2h, on='cluster', how='outer')
            df3 = pd.merge(df3, df1h, on='cluster', how='outer')

            df3 = df3.fillna(0)
            df3 = df3.astype({"eve_3h": int, "eve_3h_g": int, "eve_2h": int,
                              "eve_2h_g": int,"eve_1h": int, "eve_1h_g": int })

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


def insert_en_train_1_db(tb, df):
    if not LOCAL:
        if connection.is_connected():

            df.to_sql(tb, con=connection, if_exists='replace')


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

    # calculo de db_datos_trafico
    print(datetime.now(), ' Calculando datos trafico', end='\r')
    df_trafico = calcular_de_datos_trafico(fecha)

    #calculo de db_festivos
    print(datetime.now(), 'Calculando festivos      ', end='\r')
    df_fecha = calcular_de_fecha(fecha)

    # calculo de db_eventos
    print(datetime.now(), 'Calculando eventos       ', end='\r')
    df_eventos = calcular_de_eventos(fecha)

    # merge de todos los dataframes
    df = pd.merge(df_coches, df_trafico, on='cluster', how='outer')
    df = pd.merge(df, df_fecha, on='cluster', how='outer')
    df = pd.merge(df, df_eventos, on='cluster', how='outer')

    # escribir en bdd train_1
    print(datetime.now(), 'Escribiendo en dbb       \r', end='\r')

    insert_en_train_1_db(tb, df)


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