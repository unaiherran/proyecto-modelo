from secret import *

from fill_predict_table import predict
from create_training_data import calculo_parametros_un_train
from datetime import datetime
import pandas as pd
from time import sleep

drop = ['num_cars_mean', 'num_cars_median', 'num_cars_mean_woo', 'num_cars_median_woo'
        'car_min', 'car_max', 'car_mean', 'car_median',
        'car_min_woo', 'car_max_woo', 'car_mean_woo',
        'car_median_woo']

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