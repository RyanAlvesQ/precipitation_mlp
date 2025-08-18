import os
import glob
import xarray as xr
from datetime import datetime

class Reader:

    def read_files_paths_precipitation(download_folder_path):
        """
            Collects the paths of files present in the download folder
        """
        files_path_list = []
        bloqueados = [
        "202404", "202402", "202403","202401",
        "202301","202302", "202303", "202304", "202305", "202306", "202307", "202308", "202309", "202310", "202311", "202312"
        ]
        try:

            for file in os.listdir(download_folder_path):
                if not any(b in file for b in bloqueados):
                    file_path = os.path.join(download_folder_path, file)
                    files_path_list.append(file_path)

        except Exception as error:
            print(f"Erro ao verificar arquivos na pasta {download_folder_path}: {error}")

        return sorted(files_path_list)

    
    def read_dataset_precipitation(nc_file):
        """
            Open the prec dataset
        """
        try:
            with xr.open_dataset(nc_file) as dataset:
                prec_data = dataset['prec'].load()
                return prec_data

        except Exception as error:
            print(f"Erro ao abrir arquivo {nc_file}: {error}")
            return None

