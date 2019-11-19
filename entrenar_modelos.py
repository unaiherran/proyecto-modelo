# -*- coding: utf-8 -*-
"""entrenar_modelos.py
Basado en:
Generar Dataset.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1wL0Cr3hes47xf9QLrKNa_3_0pd7tGI9n
"""

from secret import *

import pandas as pd
import numpy as np
from numpy import concatenate

import mysql.connector
from sqlalchemy import create_engine

import time
from datetime import datetime
import argparse

from math import sqrt

from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.callbacks import EarlyStopping

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from sklearn.externals import joblib

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

    engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_database}',
                           echo=False)


def entrenar_cluster(num_cluster, num_celdas_LSTM=50, epochs=200, patience=10, keep=['all'], label='label',
                     var_obj='ocu_mean', save=False):

    sql = f"SELECT * FROM train_1 where cluster={num_cluster};"
    df = pd.read_sql(sql, con=connection)
    """Cluster e index no me aportan nada"""

    df.drop('cluster', axis=1, inplace=True)
    df.drop('index', axis=1, inplace=True)

    """Fecha al principio y ordenado por fecha (esto lo haré en la consulta SQL)"""

    df.insert(0, 'fecha', df.pop("fecha"))

    df['fecha'] = pd.to_datetime(df['fecha'])

    df = df.sort_values(['fecha'], ascending=1)

    """Todos los campos del DF a números"""

    df = df.apply(pd.to_numeric)

    """Me cargo las columans que no me valen"""

    df = df.drop(['fecha', 'num_cars_min', 'num_cars_max',
                  'num_cars_min_woo', 'num_cars_max_woo',
                  'int_min', 'int_max', 'ocu_min', 'ocu_max',
                  'car_min', 'car_max', 'int_min_woo', 'int_max_woo',
                  'ocu_min_woo', 'ocu_max_woo',
                  'car_min_woo', 'car_max_woo'], axis=1)

    """# Entrenando con menos datos, y guardandome otros para validzr al final"""

    if keep == ['all']:
        df1 = df
    else:
        df1 = df[keep]

    """Campo objetivo es OCU+1"""
    df1['var_obj'] = df1[var_obj].shift(-1)
    #df1['ocu+1'] = df1.ocu_mean.shift(-1)

    """Quito la última fila..."""

    df1 = df1.drop([df1.shape[0] - 1])

    # Quito desconocidos
    df1 = df1.replace(999999.0, np.nan)

    #Cambio NA por 0
    df1 = df1.fillna(0)

    values = df1.values
    values = values.astype('float32')

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled = scaler.fit_transform(values)

    n_train = int(scaled.shape[0] * 0.65)
    n_test = int(scaled.shape[0] * 0.25)
    n_val = int(scaled.shape[0] * 0.10) #no vale para nada, sólo lo dejo para saber que el conjunto de Val es un 10%

    train = scaled[:n_train, :]
    resto = scaled[n_train:, :]
    test = resto[:n_test, :]
    val = resto[n_test:, :]

    train_X, train_y = train[:, :-1], train[:, -1]
    test_X, test_y = test[:, :-1], test[:, -1]
    val_X, val_y = val[:, :-1], val[:, -1]

    guarda_y = val_y

    # reshape input to be 3D [samples, timesteps, features]
    # Necesario para dar de comer al LSTM

    train_X = train_X.reshape((train_X.shape[0], 1, train_X.shape[1]))
    test_X = test_X.reshape((test_X.shape[0], 1, test_X.shape[1]))
    val_X = val_X.reshape((val_X.shape[0], 1, val_X.shape[1]))

    guarda_X = val_X

    # Modelo
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=0, patience=patience)

    model = Sequential()
    model.add(LSTM(num_celdas_LSTM, input_shape=(train_X.shape[1], train_X.shape[2])))
    model.add(Dense(1))
    model.compile(loss='mae', optimizer='adam')

    # Entrenar modelo
    history = model.fit(train_X, train_y, epochs=epochs, batch_size=72, validation_data=(test_X, test_y),
                        shuffle=False, verbose=0, callbacks=[es])

    # evaluar modelo

    # make a prediction
    yhat = model.predict(guarda_X)

    # invert scaling for forecast
    val_X_1 = guarda_X.reshape((guarda_X.shape[0], guarda_X.shape[2]))
    inv_yhat = concatenate((val_X_1, yhat), axis=1)
    inv_yhat_1 = scaler.inverse_transform(inv_yhat)
    inv_yhat_1 = inv_yhat_1[:, -1]

    # Valores de validacion (reescalar para evaluar modelo)
    val_y = val_y.reshape((len(val_y), 1))
    inv_y = concatenate((val_X[:, 0, :], val_y), axis=1)
    inv_y = scaler.inverse_transform(inv_y)
    inv_y = inv_y[:, -1]

    # calculate RMSE

    rmse = sqrt(mean_squared_error(inv_y, inv_yhat_1))

    print(f'RMSE: {rmse}')

    # TODO Guardar resultados
    model_name = f'{label}_cluster_{num_cluster}_obj_{var_obj}'

    row = {}
    row['cluster'] = num_cluster
    row['label'] = label
    row['keep'] = keep
    row['model_name'] = model_name
    row['epochs'] = epochs
    row['stopped_epoch'] = es.stopped_epoch
    row['rmse'] = rmse
    row['training_var'] = var_obj

    try:
        resultado_df = pd.read_csv('resultado.csv')
        resultado_df.drop('Unnamed: 0', axis=1, inplace=True)
        resultado_df = resultado_df.append(row, ignore_index=True)

    except FileNotFoundError:
        # crear el df
        resultado_df = pd.DataFrame(columns=['cluster', 'label', 'keep', 'model_name', 'training_var',
                                             'epochs', 'stopped_epoch', 'rmse'])
        print('Resultado.csv no encontrado')
        resultado_df = resultado_df.append(row, ignore_index=True)

    resultado_df.to_csv('resultado.csv')

    if save:
        model.save(f'models/model_{num_cluster}.h5')
        # grabar scaler
        joblib.dump(scaler, f'models/scaler_{num_cluster}.job')


    # Dataframe para evaluar
    df_y = pd.DataFrame(data=inv_y.tolist(), columns=['y'])
    df_y['y_pred'] = inv_yhat_1




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

    variables_objetivo = ['ocu_mean', 'ocu_median']

    for cl in range(initial, final):
        for vobj in variables_objetivo:
            now = datetime.now()
            print(f'{now} - Entrenando cluster {cl} para target: {vobj}')
            entrenar_cluster(cl, var_obj=vobj, save=True)
            time.sleep(1)


if __name__ == '__main__':
    main()
