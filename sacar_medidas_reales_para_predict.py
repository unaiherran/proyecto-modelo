from secret import *
import mysql.connector

import pandas as pd

from create_training_data import calculo_parametros_un_train

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

            primer_reg_vacio = cur.fetchall()

            if primer_reg_vacio:
                fecha_a_buscar = primer_reg_vacio[0][2]
                df = calculo_parametros_un_train(fecha_a_buscar, save_in_db=False)

                df = df[['cluster', 'ocu_mean']]

                sql = f'SELECT id_pred, cluster FROM predict where fecha="{fecha_a_buscar}"'
                cur.execute(sql)

                lista_de_reg_a_actualizar = cur.fetchall()

                for reg in lista_de_reg_a_actualizar:
                    cluster = reg[1]
                    ocu_medida = df.loc[df['cluster'] == cluster].values[0][1]

                    print(ocu_medida)
                    sleep(60)

                print(df)

                #print(lista_de_reg_a_actualizar)


            sleep(60)

if __name__ == '__main__':
    main()