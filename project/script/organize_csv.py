import os
import shutil
from datetime import datetime

def is_valid_date_format(name):
    try:
        datetime.strptime(name, '%Y%m%d')
        return True
    except ValueError:
        return False

def organize_csv_and_metrics(directory):
    """
    Organiza os arquivos com base na Data atual e nome da tabela.

    Args:
        directory (str): Diretório onde os arquivos CSV estão localizados.
    """

    # Obtém a data de hoje no formato YYYYMMDD
    today_date = datetime.today().strftime('%Y%m%d')

    # Diretório de destino para os arquivos de hoje
    today_directory = os.path.join(directory, today_date)
    os.makedirs(today_directory, exist_ok=True)

    # Diretório de destino para o arquivo job_metrics.json de hoje
    metrics_directory = os.path.join(today_directory, "metrics")
    os.makedirs(metrics_directory, exist_ok=True)

    # Lista todos os arquivos no diretório que não são diretórios com nomes no formato de data nem "current_data"
    files_and_dirs = os.listdir(directory)
    files = [f for f in files_and_dirs if os.path.isfile(os.path.join(directory, f)) and not is_valid_date_format(f) and f != "current_data"]

    for file in files:
        src = os.path.join(directory, file)

        if file == "job_metrics.json":
            dst = os.path.join(metrics_directory, file)
        else:
            # Extrai o prefixo até o primeiro traço (assumindo um formato de nome consistente)
            file_prefix = '-'.join(file.replace('-', '_').split('_')[0:-1])

            # Cria o diretório do prefixo dentro do diretório de hoje
            prefix_directory = os.path.join(today_directory, file_prefix)
            os.makedirs(prefix_directory, exist_ok=True)

            # Constrói o nome do arquivo de destino com o nome desejado
            dst = os.path.join(prefix_directory, f"{file_prefix}.csv")

        # Move o arquivo para o destino (substitui se existir)
        shutil.move(src, dst)

    # Remove o diretório original, se estiver vazio
    if not os.listdir(directory):
        os.rmdir(directory)

if __name__ == "__main__":
    # Diretório a ser organizado
    directory_to_organize = "/code-challenge/data/data-pipeline"
    organize_csv_and_metrics(directory_to_organize)
