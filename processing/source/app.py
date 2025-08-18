import os
import numpy as np
import dask
from datetime import datetime
from config.config_reader import ConfigReader
from config.db_config import Connection
from models.reader import Reader
from models.conversor import Conversor
from models.data_clipper import PrecipitationCutter
from models.extract_date import ExtractDate
from repository.data import Data
from collections import defaultdict

# Config.yaml reader
config_reader = ConfigReader()
app_config = config_reader.read_config("./config/config.yaml")
params = app_config.get('configuration_parameters', [])

def get_params(param_name):
    return next((param.get(param_name) for param in params if param_name in param), None)

# Configuration parameters
download_folder_path = get_params('download_folder_path')
municipalities_shapefile_path = get_params('municipalities_shapefile_path')
database_host = get_params('database_host')
database_port = get_params('database_port')
database_name = get_params('database_name')
database_user = get_params('database_user')
database_password = get_params('database_password')


def precipitation_processing():
    connection = Connection.db_connection(database_host, database_port, database_name, database_user, database_password)

    Data.ensure_tables_exist(connection)

    files = Reader.read_files_paths_precipitation(download_folder_path)

    for file_path in files:
        file_name = os.path.basename(file_path)

        print(f"Processando novo arquivo: {file_name}")

        dataset = Reader.read_dataset_precipitation(file_path)

        converted_dataset = Conversor.longitude_adjuster(dataset)
        adjusted_dataset = Conversor.attribute_adjuster(converted_dataset)

        file_date = ExtractDate.get_datetime_from_filename(file_name)

        municipalities_statistics = PrecipitationCutter.municipalities_cutter(adjusted_dataset, municipalities_shapefile_path, file_date)

        insert_data_municipalities = Data.insert_municipalities_precipitation_data(municipalities_statistics, connection, file_name, file_date)

        if insert_data_municipalities["status"] == "ok":
            print(insert_data_municipalities["message"])
        else:
            print(f"Erro ao processar arquivo {file_name}.")

if __name__ == "__main__":
    precipitation_processing()