from secret import *

import fill_predict_table
from create_training_data import calculo_parametros_un_train
from datetime import datetime
import pandas as pd
from time import sleep


def main():
    hora_de_calculo = pd.Timestamp.now().floor('15min').to_pydatetime()

    # calcular dataset para el ultimo 15 min
    df = calculo_parametros_un_train(hora_de_calculo, save_in_db=False)
    print(df)
    # predecir
    pass

    # Escribir en BDD
    pass


if __name__ == '__main__':
    main()