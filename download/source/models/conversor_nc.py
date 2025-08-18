import os
import subprocess
from glob import glob

class Conversor_nc:
	def convert_to_nc(download_folder_path):
		converted_files = []
		
		# Lista todos os arquivos .grib2 no diretório que começam com MERGE_CPTEC_
		grib_files = sorted(glob(os.path.join(download_folder_path, "MERGE_CPTEC_*.grib2")))

		if not grib_files:
			print("Nenhum arquivo .grib2 encontrado para conversão.")
			return converted_files

		for download_path in grib_files:
			full_file_name = os.path.basename(download_path)
			file_name = full_file_name[:-6] + ".nc"
			output_file = os.path.join(download_folder_path, file_name)

			try:
				# Executa a conversão
				result = subprocess.run(
					["cdo", "-f", "nc", "copy", download_path, output_file], 
					check=True, 
					capture_output=True,
					text=True
				)

				# Verifica se a conversão foi bem-sucedida
				if result.returncode == 0 and os.path.exists(output_file):
					os.remove(download_path)
					converted_files.append(file_name)
				else:
					print(f"Falha na conversão: {output_file} não foi gerado. Erro: {result.stderr}")
			
			except FileNotFoundError:
				print("Erro: O comando 'cdo' não foi encontrado. Certifique-se de que o CDO está instalado e acessível no PATH.")
			except subprocess.CalledProcessError as e:
				print(f"Erro ao converter {download_path} para .nc: {e.stderr}")
			except Exception as e:
				print(f"Erro inesperado ao converter {download_path}: {e}")

		return converted_files