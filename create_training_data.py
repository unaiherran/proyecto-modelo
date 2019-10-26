from datetime import datetime

import mysql.connector


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

    return intensidad, ocupacion, carga



def calcular_de_eventos(cluster, fecha):
    return True


def calcular_de_fecha(fecha):
    dia_semana = fecha.weekday()
    dia = fecha.day
    mes = fecha.month
    anyo = fecha.year

    if connection.is_connected():
        cur = connection.cursor();

        sql = f"SELECT * FROM Festivos where fecha=str_to_date('{fecha.year}-{fecha.month}-{fecha.day}', '%Y-%m-%d');"

        cur.execute(sql)

        data = cur.fetchall()

        print(data)

    festivo = True

    return dia_semana, dia, festivo


def poblar_train(cluster, fecha):
    # Fecha es dattime
    # Cluster es int

    # calculo de db_imagenes_camara
    num_coches = calcular_de_imagenes_camara(cluster, fecha)

    # calculo de db_datos_trafico
    intensidad, ocupacion, carga = calcular_de_datos_trafico(cluster, fecha)

    #calculo de db_festivos
    dia_semana, dia_mes, festivo = calcular_de_fecha(fecha)

    # calculo de db_eventos

    pass
# calculo de parametros

# q a  datos trafico
# calculo de parametros

# q a festivos

# escribir en bdd train_1


def main():
    fecha = datetime.strptime("12-10-19", "%d-%m-%y")
    cluster = 1
    poblar_train(cluster, fecha)

if __name__ == '__main__':
    main()