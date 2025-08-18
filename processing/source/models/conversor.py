import numpy as np
import xarray as xr

class Conversor:
    def longitude_adjuster(converted_unit_data):
        """
            Adjusts the longitude range from 0°-360° to -180°-180°
        """
        adjusted_longitudes = converted_unit_data.assign_coords(
        lon=((converted_unit_data.lon + 180) % 360) - 180)

        ordered_longitudes = adjusted_longitudes.sortby('lon')
        return ordered_longitudes
    
    def attribute_adjuster(attribute_data):

        prec = xr.DataArray(
            attribute_data,
            attrs={'long_name': 'Precipitation', 'units': 'kg m**-2', 'param': '5.15.0'}
        )
        prec.attrs['long_name'] = 'Precipitação acumulada'
        prec.attrs['units'] = 'mm'
        return prec