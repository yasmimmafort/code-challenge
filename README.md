# Indicium Tech Code Challenge

Passo a Passo para Utilizar os DAGs do Airflow
1. Clonar o Repositório

Primeiro, clone o repositório do GitHub que contém os DAGs do Airflow e outros arquivos necessários:

```bash

git clone https://github.com/yasmimmafort/code-challenge.git
cd code-challenge
```

2. Iniciar os Serviços com Docker Compose

Use Docker Compose para iniciar os serviços necessários, como PostgreSQL (origem e destino) e Airflow:

```bash
docker-compose up -d
```

Este comando irá levantar três containers:

    db: Banco de dados PostgreSQL utilizado como fonte dos dados.
    db-2: Banco de dados PostgreSQL onde os dados transformados serão carregados.
    airflow: Servidor Airflow que irá orquestrar os DAGs.

3. Acessar o Airflow

Abra seu navegador e vá para http://localhost:8080.

    Credenciais de Acesso: Use as credenciais padrão:
        Usuário: airflow
        Senha: airflow

4. Habilitar e Executar as DAGs

No painel do Airflow, você encontrará três DAGs principais:

    pipeline1: Executa o pipeline1 do Meltano.
    pipeline2: Executa o pipeline2 do Meltano.
    pipeline3: Executa o pipeline1 seguido pelo pipeline2.

Para cada DAG:

    Habilitar: Se uma DAG ainda não estiver habilitada, clique no botão "Enable" para habilitá-la.
    Executar: Clique no botão "Trigger DAG" para iniciar a execução da DAG manualmente.

5. Monitorar e Verificar Logs

Após iniciar uma DAG, você pode monitorar o progresso e verificar os logs na interface do Airflow. Isso ajudará a garantir que os pipelines estão sendo executados conforme esperado.

6. Ajustes e Configurações

    Configurações de Caminho e Comando: Se necessário, ajuste os caminhos e comandos nos arquivos de DAG (Airflow/dags/) para corresponder ao seu ambiente específico.

7. Parar os Serviços

Quando terminar, você pode parar os containers Docker usando o comando:

```bash

docker-compose down
```

Isso desligará e removerá os containers, mas preserve os dados persistidos conforme configurado no Docker Compose.
