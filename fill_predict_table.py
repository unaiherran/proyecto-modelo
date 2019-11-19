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

import pandas as pd
import numpy as np


connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_passwd,
        database=db_database,
        port=db_port
    )

engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_database}',
                        echo=False)


def predict(num_cluster, table='predict'):

    # cargar modelo
    model = load_model(f'models/model_{num_cluster}.h5')
    scaler = joblib.load(f'models/scaler_{num_cluster}.job')

    sql = f"SELECT * FROM train_1 where cluster={num_cluster}"

    # cargar datos
    df = pd.read_sql(sql, con=connection)

    df.drop('cluster', axis=1, inplace=True)
    df.drop('index', axis=1, inplace=True)

    df = df.sort_values(['fecha'], ascending=1)

    df = df.apply(pd.to_numeric)

    df1 = df.drop(['fecha', 'num_cars_min', 'num_cars_max',
                  'num_cars_min_woo', 'num_cars_max_woo',
                  'int_min', 'int_max', 'ocu_min', 'ocu_max',
                  'car_min', 'car_max', 'int_min_woo', 'int_max_woo',
                  'ocu_min_woo', 'ocu_max_woo',
                  'car_min_woo', 'car_max_woo'], axis=1)


    # sacar values
    values = df1.values
    values = values.astype('float32')

    # escalar
    scaled = scaler.transform(values)

    # predecir
    yhat = model.predict(scaled)

    # desescalar

    val_X_1 = scaled.reshape((scaled.shape[0], scaled.shape[2]))
    inv_yhat = np.concatenate((val_X_1, yhat), axis=1)
    inv_yhat_1 = scaler.inverse_transform(inv_yhat)
    inv_yhat_1 = inv_yhat_1[:, -1]

    # escribir en tabla predict
    df['predict'] = inv_yhat_1.tolist()

    df.to_csv('evaluar.csv')


