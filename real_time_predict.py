from secret import *

from fill_predict_table import predict
from create_training_data import calculo_parametros_un_train
from datetime import datetime
import pandas as pd
import numpy as np
from time import sleep

from keras.models import load_model
from sklearn.externals import joblib
import keras


drop = ['num_cars_mean', 'num_cars_median', 'num_cars_mean_woo', 'num_cars_median_woo'
        'car_min', 'car_max', 'car_mean', 'car_median',
        'car_min_woo', 'car_max_woo', 'car_mean_woo',
        'car_median_woo']


def predict(num_cluster, data_to_predict, drop=['none'], modelo='ocu_mean', target_var='ocu_mean', save_in_db=True, ):
    # cargar modelo
    model = load_model(f'models/model_{num_cluster}_{modelo}.h5')
    scaler = joblib.load(f'models/scaler_{num_cluster}_{modelo}.job')

    df = data_to_predict

    df.drop('index', axis=1, inplace=True)
    df1 = df.drop(['fecha', 'num_cars_min', 'num_cars_max',
                  'num_cars_min_woo', 'num_cars_max_woo',
                  'int_min', 'int_max', 'ocu_min', 'ocu_max',
                  'car_min', 'car_max', 'int_min_woo', 'int_max_woo',
                  'ocu_min_woo', 'ocu_max_woo',
                  'car_min_woo', 'car_max_woo', 'cluster'], axis=1, errors='ignore')

    df1 = df1.apply(pd.to_numeric)

    if drop != ['none']:
        df1 = df1.drop(drop, axis=1, errors='ignore')

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

    df = df[['cluster', 'fecha', target_var, 'ocu_pred']]

    #la prediccion se tiene que guardar en la siguiente fila.
    df['ocu_pred'] = df['ocu_pred'].shift(1)

    df = df.rename(columns={"ocu_mean": "ocu_real", "predict": "ocu_pred"})

    keras.backend.clear_session()

    return inv_yhat_1



def main():
    hora_de_calculo = pd.Timestamp.now().floor('15min').to_pydatetime()
    ahora = datetime.now()
    print(hora_de_calculo, ahora)
    # calcular dataset para el ultimo 15 min
    df = calculo_parametros_un_train(hora_de_calculo, save_in_db=False)
    # predecir
    modelo = 'ocu_mean_no_cars_no_car'
    for cl in range(0,200):
        data_to_predict = df.iloc[cl]
        print(f"Cluster: {cl}")
        print (data_to_predict)
        prediction=predict(cl, drop=drop, save_in_db=False, modelo=modelo, data_to_predict=data_to_predict)
        print(prediction)
    # Escribir en BDD
    pass


if __name__ == '__main__':
    main()