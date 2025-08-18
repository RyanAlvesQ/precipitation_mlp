import geopandas as gpd
import rioxarray
import pandas as pd
import numpy as np
from datetime import datetime 
# -- imports --

class PrecipitationCutter:
    def municipalities_cutter(dataset, shapefile_path, data_arquivo):
        """
        Cut the data for each municipality and calculate the daily average.
        """
        municipalities_statistics = []
        try:
            
            # load the shapefiles
            shape_brazil = gpd.read_file(shapefile_path)

            # verifying and adjusting the system with coordenate references (CRS)
            if shape_brazil.crs != "EPSG:4326":
                shape_brazil = shape_brazil.to_crs("EPSG:4326")  # Ensure that the CRS is in EPSG:4326

            dataset = dataset.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace =True)

            # ensure the dataset to have the right CRS (EPSG:4326)
            daily_data = dataset.rio.write_crs("EPSG:4326")

            data_acc = daily_data.sum(dim="time", keep_attrs=True)  

            # process data
            for _, municipality in shape_brazil.iterrows():
                municipality_name = municipality['NM_MUNICIP'] 
                municipality_code = municipality['CD_GEOCMU']
                
                lat = municipality.geometry.y                
                lon = municipality.geometry.x  
                
                try:
                    
                    # Interpolation to calculate value in the urban area
                    interpolated_accumulated = data_acc.interp(lat=lat, lon=lon, method="linear").compute().values
                    
                    interpolated_accumulated = float(interpolated_accumulated)

                    if interpolated_accumulated is not None:
                        municipalities_statistics.append({
                            "municipality": municipality_name,
                            "CD_MUN": municipality_code,
                            "state": municipality['NM_ESTADO'],
                            "accumulated_prec": round(interpolated_accumulated, 3),
                            "date": data_arquivo.strftime("%Y-%m-%d %H:%M:%S")
                        })
                    
                    else:
                        print(f"Não foi possível encontrar dado válido para {municipality_name}.")
            
                except Exception as e:
                    print(f"Erro ao processar o município {municipality_name} ({lat}, {lon}): {e}")
            return municipalities_statistics

        except Exception as e:
            print("Erro ao fazer o recorte dos dados.", e)
            return None
        
