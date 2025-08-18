from config.config_reader import ConfigReader
from models.prec_downloader import PrecipitationDownloader

# Leitura do arquivo de configuração
config_reader = ConfigReader()
app_config = config_reader.read_config("./config/config.yaml")
params = app_config.get('configuration_parameters', [])

# Função auxiliar para extrair parâmetros do YAML
def get_params(param_name):
    return next((param.get(param_name) for param in params if param_name in param), None)

def main():
    # Recupera parâmetros da configuração
    url_base_precipitation = get_params('url_base_precipitation')
    download_folder_path = get_params('download_folder_path')

    # Entrada dinâmica do usuário
    try:
        ano = int(input("Digite o ano (ex: 2024): "))
        mes = int(input("Digite o mês (1-12): "))
        if mes < 1 or mes > 12:
            raise ValueError("Mês inválido. Digite um valor entre 1 e 12.")
    except ValueError as e:
        print(f"❌ Erro de entrada: {e}")
        return

    print(f"\n📦 Iniciando o download de arquivos GRIB2 para {ano}-{mes:02d}...\n")

    # Executa o processo de download e conversão
    resultados = PrecipitationDownloader.download_and_convert(
        url_base_precipitation=url_base_precipitation,
        download_folder_path=download_folder_path,
        ano=ano,
        mes=mes
    )

    # Resultado final
    if resultados:
        print(f"\n✅ Total de arquivos convertidos: {len(resultados)}")
    else:
        print("\n⚠️ Nenhum novo arquivo foi processado.")

if __name__ == "__main__":
    main()
