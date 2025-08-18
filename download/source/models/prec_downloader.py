from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import os
import time
from models.conversor_nc import Conversor_nc
from datetime import datetime, timedelta


class PrecipitationDownloader:
    @staticmethod
    def generate_daily_urls(base_url, ano, mes):
        urls = []
        data = datetime(ano, mes, 1)
        url = f"{base_url}/{data.year}/{data.month:02d}/"
        urls.append(url)

        return urls

    @staticmethod
    def download_and_convert(url_base_precipitation, download_folder_path, ano, mes):
        urls = PrecipitationDownloader.generate_daily_urls(url_base_precipitation, ano, mes)
        total_downloads = []

        if not os.path.exists(download_folder_path):
            os.makedirs(download_folder_path)

        for url in urls:
            print(f"Acessando: {url}")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Erro ao acessar {url}: {e}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all('a')

            # Filtrar apenas os arquivos .grib2
            grib_links = [link.get('href') for link in links if link.get('href', '').endswith('.grib2')]

            if not grib_links:
                print(f"⚠️ Nenhum arquivo .grib2 encontrado em {url}")
                continue

            for grib_file in grib_links:
                file_url = urljoin(url, grib_file)
                file_path = os.path.join(download_folder_path, grib_file)

                print(f"⬇️ Baixando {grib_file} de {file_url}")
                try:
                    with requests.get(file_url, stream=True, timeout=15) as file_response:
                        if file_response.status_code == 200:
                            with open(file_path, 'wb') as f:
                                for chunk in file_response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            print(f"✔️ Download concluído: {file_path}")
                            total_downloads.append(grib_file)
                        else:
                            print(f"❌ Erro ao baixar {grib_file}: {file_response.status_code}")
                except Exception as e:
                    print(f"❌ Erro ao baixar {grib_file}: {e}")

            time.sleep(10)  # Para evitar sobrecarga no servidor

        # ✅ Após os downloads, converter com CDO
        print("\n🔄 Iniciando conversão para .nc...")
        converted_files = Conversor_nc.convert_to_nc(download_folder_path)

        print(f"\n✅ Conversão finalizada. Arquivos convertidos: {len(converted_files)}")
        return converted_files if converted_files else None
