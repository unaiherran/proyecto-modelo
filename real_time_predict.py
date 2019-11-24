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
    print(df)
    # predecir
    pass
    predict(42, drop=drop, save_in_db=False, data_to_predict=[1,2])
    # Escribir en BDD
    pass


if __name__ == '__main__':
    main()