# -*- coding: utf-8 -*-
"""fill_predict_table.py
Basado en los modelos que tenemos, realizar predicciones para todo el dataset de entrenamiento
y guardarlo en la tabla predict:
Generar Dataset.ipynb

"""

from secret import *
import mysql.connector
from sqlalchemy import create_engine

from keras.models import load_model
from sklearn.externals import joblib
import keras

import pandas as pd
import numpy as np

import argparse

from math import sqrt
from sklearn.metrics import mean_squared_error

connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_passwd,
        database=db_database,
        port=db_port
    )

engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_database}',
                        echo=False)


def predict(num_cluster, in_table='train_1', out_table='predict', drop=['none'], modelo='ocu_mean'):

    # cargar modelo
    model = load_model(f'models/model_{num_cluster}_{modelo}.h5')
    scaler = joblib.load(f'models/scaler_{num_cluster}_{modelo}.job')

    sql = f"SELECT * FROM {in_table} where cluster={num_cluster}"

    # cargar datos
    df = pd.read_sql(sql, con=connection)

    df.drop('index', axis=1, inplace=True)

    df = df.sort_values(['fecha'], ascending=1)

    df1 = df.drop(['fecha', 'num_cars_min', 'num_cars_max',
                  'num_cars_min_woo', 'num_cars_max_woo',
                  'int_min', 'int_max', 'ocu_min', 'ocu_max',
                  'car_min', 'car_max', 'int_min_woo', 'int_max_woo',
                  'ocu_min_woo', 'ocu_max_woo',
                  'car_min_woo', 'car_max_woo', 'cluster'], axis=1)

    df1 = df1.apply(pd.to_numeric)
    if drop != ['none']:
        df1 = df1(drop, axis=1)

    # sacar values
    values = df1.values
    values = values.astype('float32')

    # escalar
    zeros = np.zeros((values.shape[0], 1))  # 2 is a number of rows in your array.
    values = np.hstack((values, zeros))
    scaled = scaler.transform(values)

    scaled = np.delete(scaled, -1,1)

    # predecir
    scaled = scaled.reshape((scaled.shape[0], 1, scaled.shape[1]))
    yhat = model.predict(scaled)

    # desescalar

    val_X_1 = scaled.reshape((scaled.shape[0], scaled.shape[2]))
    inv_yhat = np.concatenate((val_X_1, yhat), axis=1)
    inv_yhat_1 = scaler.inverse_transform(inv_yhat)
    inv_yhat_1 = inv_yhat_1[:, -1]

    # escribir en tabla predict
    df['ocu_pred'] = inv_yhat_1.tolist()

    df = df[['cluster', 'fecha', 'ocu_mean', 'ocu_pred']]

    #la prediccion se tiene que guardar en la siguiente fila.
    df['ocu_pred'] = df['ocu_pred'].shift(1)

    df = df.rename(columns={"ocu_mean": "ocu_real", "predict": "ocu_pred"})

    df.to_sql(name=out_table, con=engine, if_exists='append', index=False)

    keras.backend.clear_session()


def main():
    descripcion = 'Esta es la descripcion'
    parser = argparse.ArgumentParser(description=descripcion)
    parser.add_argument("-i", "--i", help="initial cluster")
    parser.add_argument("-e", "--e", help="final cluster")

    args = parser.parse_args()

    if args.i:
        initial = int(args.i)
    else:
        initial = 0

    if args.e:
        final = int(args.e)
    else:
        final = 200
    print(initial, final)
    # drop = ['num_cars_mean', 'num_cars_median', 'num_cars_mean_woo', 'num_cars_median_woo']

    for cl in range(initial, final):
        print(f'Prediciendo cluster {cl}')
        predict(cl, in_table='test_data_1', out_table='predict_median', modelo='ocu_median')


if __name__ == '__main__':
    main()

