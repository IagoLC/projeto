from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import csv
import os

def fetch_covid_data():
    url = "https://imunizacao-es.saude.gov.br/_search"
    username = "imunizacao_public"
    password = "qlto5t&7r_@+#Tlstigi"

    # Define os parâmetros de autenticação
    auth = (username, password)

    # Define o corpo da requisição inicial
    request_body = {
        "size": 10000
    }

    # Faz uma requisição POST para obter os dados JSON
    response = requests.post(url, auth=auth, json=request_body)

    # Verifica se a requisição foi bem-sucedida (código 200 indica sucesso)
    if response.status_code == 200:
        data = response.json()  # Converte os dados JSON em um objeto Python

        # Obtém o cabeçalho
        header = list(data.get("hits", {}).get("hits", [])[0].get("_source", {}).keys())

        # Define o caminho para salvar o arquivo CSV dentro do diretório do Airflow
        airflow_home = os.environ.get("AIRFLOW_HOME")
        csv_path = os.path.join(airflow_home, "data", "dados_covid.csv")

        # Cria o diretório se ele não existir
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        # Cria um arquivo CSV para escrita
        with open(csv_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # Escreve o cabeçalho no arquivo CSV
            writer.writerow(header)

            # Processa os dados da primeira página
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                row = [source.get(field) for field in header]

                # Escreve a linha de dados no arquivo CSV
                writer.writerow(row)

            # Verifica se há mais páginas de dados
            scroll_id = data.get("_scroll_id")
            while scroll_id:
                # Define o corpo da requisição para obter a próxima página
                scroll_request_body = {
                    "scroll_id": scroll_id,
                    "scroll": "1m"
                }

                # Faz uma nova requisição POST para obter a próxima página
                response = requests.post(f"{url}/scroll", auth=auth, json=scroll_request_body)

                # Verifica se a requisição foi bem-sucedida (código 200 indica sucesso)
                if response.status_code == 200:
                    data = response.json()  # Converte os dados JSON em um objeto Python

                    # Processa os dados da página atual
                    for hit in data.get("hits", {}).get("hits", []):
                        source = hit.get("_source", {})
                        row = [source.get(field) for field in header]

                        # Escreve a linha de dados no arquivo CSV
                        writer.writerow(row)

                    scroll_id = data.get("_scroll_id")
                else:
                    print("Falha ao obter os dados da próxima página.")
                    break
    else:
        print("Falha ao obter os dados. Verifique a URL ou as credenciais de acesso.")

    return csv_path


def copy_csv_to_windows():
    docker_command = "docker cp 1e264e65910b:/opt/airflow/data/dados_covid.csv C:\\xampp\\htdocs\\arquitetura.proj\\airflow-docker\\base_covid19\\dados_covid.csv"
    os.system(docker_command)


default_args = {
    'start_date': datetime(2023, 5, 15)
}

dag = DAG(
    'a_fetch_covid_data_dag',
    default_args=default_args,
    schedule_interval='@once'
)

fetch_task = PythonOperator(
    task_id='a_fetch_covid_data_task',
    python_callable=fetch_covid_data,
    dag=dag
)

copy_task = PythonOperator(
    task_id='copy_csv_to_windows_task',
    python_callable=copy_csv_to_windows,
    dag=dag
)

fetch_task >> copy_task
