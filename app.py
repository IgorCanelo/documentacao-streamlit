import streamlit as st
import pandas as pd

df_login_agg_date = pd.read_csv(r"tables\login_agg_date.csv", sep=",", index_col=0)
df_first_time_login = pd.read_csv(
    r"tables/first_time_login.csv",
    sep=",",
    index_col=0,
    keep_default_na=False,
    na_values=[],  # evita interpretação automática de valores vazios
    dtype=str,  # força tudo como string para não perder nada
    encoding="utf-8"
)


# Controle de navegação entre as seções interativas
if "page" not in st.session_state:
    st.session_state.page = "📌 Overview"

# Função para mudar de página
def change_page(page_name):
    st.session_state.page = page_name
    st.rerun()


# Sidebar
st.sidebar.title("Navegação")
nav = st.sidebar.radio("Ir para:", ["📌 Overview", "☁️ Ambiente na Nuvem", "🖥️ Ambiente Local", 
                                    "🏗️ Arquitetura de Dados"],
                                    index=["📌 Overview", "☁️ Ambiente na Nuvem", "🖥️ Ambiente Local", 
                                    "🏗️ Arquitetura de Dados"].index(st.session_state.page))

# Atualiza a página ativa com base no radio button
if nav != st.session_state.page:
    st.session_state.page = nav
    st.rerun()

# Página: Overview
if st.session_state.page == "📌 Overview":


    st.title("📌 Visão Geral do Projeto de Pipeline de Dados")

    st.markdown("""
    Este projeto implementa um pipeline completo para a simulação e análise de logins de usuários em um jogo. Para testar sua execução, siga os passos de configuração do ambiente local e cloud, descritos nas próximas seções.

    Após a conclusão do setup, a DAG estará pronta para ser executada. Em um cenário de produção, as DAGs devem ser agendadas para execução diária, garantindo que os dados estejam sempre atualizados.

    A execução do pipeline se inicia manualmente através da DAG `dag_data_creation`. A DAG `dag_data_transformation` foi desenvolvida para iniciar automaticamente ao término da primeira.

    O projeto foi estruturado visando máxima automação e tolerância a falhas. Para isso, foram implementadas notificações no Slack em casos de sucesso ou falha, enviadas para um canal específico de logs. Essa abordagem proporciona visibilidade operacional em ambientes com múltiplas DAGs, permitindo respostas rápidas e eficazes diante de falhas.

    Visualização das notificações:
    """)

    st.image("images/logs_slack.png", caption="Notificações via Slack - Airflow", use_container_width=True)

    st.markdown("---")


    st.markdown("""
    ### 🎯 Objetivo
    O principal objetivo do projeto foi disponibilizar tabelas analíticas que possam ser consumidas por analistas para gerar insights relevantes que apoiem a tomada de decisões.

    A arquitetura foi projetada para ser escalável, considerando o crescimento contínuo da base de usuários e volume de dados. Por isso, optou-se pelo uso de tecnologias como PySpark e SparkSQL, que permitem processar grandes volumes de dados de forma eficiente.
    """)

    st.markdown("---")

    st.markdown("""
    ### 🧾 Tabelas Finais Geradas

    - **`login_agg_date`**  
      Tabela agregada por data. Como os registros originais de login são baseados em timestamp, foi realizada uma agregação por data, contabilizando o número de logins por usuário a cada dia. Também foram incluídas as informações de continente, país e sistema operacional utilizados.

    """)
    st.dataframe(df_login_agg_date)

    st.markdown("""
    - **`first_time_login`**  
      Esta tabela apresenta uma agregação por `user_id`, contendo a quantidade total de logins realizados por cada usuário, bem como a data e o horário do primeiro login registrado. Além disso, inclui as informações de continente, país e sistema operacional.

    """)
    st.dataframe(df_first_time_login)

    st.markdown("""
    Reforçando: em um ambiente de produção, é essencial que as DAGs sejam executadas diariamente, garantindo a atualização contínua das tabelas analíticas.
    """)

    st.markdown("---")

    st.subheader("📁 Estrutura de Pastas do Projeto")

    st.code('''
    ├── data_pipeline_project/
    │   ├── dags/
    │   │   ├── dag_data_creation.py
    │   │   └── dag_data_transformation.py
    │   ├── spark-scripts/
    │   │   ├── create/
    │   │   │   ├── logins.py
    │   │   │   ├── user_country.py
    │   │   │   └── user_info.py
    │   │   └── transform-load/
    │   │       ├── first_time_login.py
    │   │       └── login_agg_date.py
    │   ├── sql-scripts/
    │   │   ├── copy_table/
    │   │   │   ├── copy_first_time_login.sql
    │   │   │   └── copy_login_agg_date.sql
    │   │   └── create_table/
    │   │       ├── first_time_login.sql
    │   │       ├── login_agg_date.sql
    │   │       ├── logins.sql
    │   │       ├── user_country.sql
    │   │       └── user_info.sql
    │   ├── utilitys/
    │   │   ├── __init__.py
    │   │   ├── connections.py
    │   │   └── slack_notifications.py
    │   ├── .env
    │   ├── docker-compose.yml
    │   ├── Dockerfile.airflow
    │   ├── requirements.txt
    │   └── README.md
    ''')

    st.markdown("---")
    st.subheader("✅ Pronto para iniciar?")
    if st.button("☁️ Configurar Ambiente na Nuvem"):
        change_page("☁️ Ambiente na Nuvem")
        st.rerun()

# Página: Ambiente na Nuvem
elif st.session_state.page == "☁️ Ambiente na Nuvem":
    st.title("☁️ Setup: Ambiente na Nuvem (AWS)")

    st.markdown("""
## ✍️ Introdução

Este documento apresenta os passos necessários para configurar e executar o projeto na AWS. O processo envolve três aspectos principais: estabelecer a conexão local com o Redshift Serverless, criar os buckets no S3 e definir suas respectivas políticas e configurações de segurança para permitir o acesso do Redshift aos buckets.

A adoção desses passos se deve à escolha de desenvolver os códigos utilizando PySpark. Inicialmente, utilizei a conexão direta com o Redshift por meio do driver JDBC fornecido pela AWS. Embora essa abordagem permitisse utilizar exclusivamente o Redshift, sem a necessidade de outros serviços da AWS, ela apresenta uma limitação importante, o Redshift Serverless é tarifado por consulta e tempo de execução, e o uso do JDBC faz com que o PySpark envie os dados linha por linha. Essa prática, além de extremamente ineficiente do ponto de vista de desempenho, resulta em um custo significativamente mais elevado devido ao tempo de execução para salvar os dados.

Diante disso, optei por uma abordagem mais eficiente: salvar os dados no S3 em formato Parquet, um formato muito leve e otimizado que o PySpark consegue utilizar seu processamento distribuído para o formato e, em seguida, utilizar o comando COPY para transferi-los para o Redshift. Essa estratégia é vantajosa por dois motivos principais, o Redshift é nativamente estruturado para realizar o COPY com processamento paralelo, o que garante um desempenho muito superior, e por ser mais rápido, diminui significativamente o tempo que o RedShift fica em execução, contribuindo diretamente para a redução de custos.

Portanto, os passos descritos a seguir foram definidos com o objetivo de garantir maior eficiência, desempenho e economia na execução do projeto.

---

## ✅ Passo a Passo de Configuração

### 1. Criar o Workgroup e Namespace no Redshift Serverless

- Acesse o console do Amazon Redshift → Redshift Serverless.
- Crie um novo **Workgroup** e um **Namespace**.
- Associe uma **função IAM** com permissões para acessar o S3 (detalhado no passo 4).
- Acesse o Redshift → Workgroup → Editar configurações.
- Ative a opção Publicamente acessível.

### 2. Criar o Bucket no Amazon S3

- Acesse o console do Amazon S3 e crie um bucket (ex: `final-data-game`).
- No bucket crie duas pastas (ex: `login-agg-date` e `first-time-login`).

### 3. Configurar o Grupo de Segurança (Security Group)

- Vá até o **EC2 → Security Groups**.
- Localize o grupo de segurança associado ao seu Workgroup Redshift.
- Adicione uma **regra de entrada**:
    - Tipo: PostgreSQL
    - Protocolo: TCP
    - Porta: 5439
    - Origem: My IP

### 4. Criar e Configurar a Função IAM

- Acesse o **IAM → Roles → Create Role**.
- Tipo de entidade confiável: **AWS Service**  
- Serviço: **Redshift**  
- Tipo: **Redshift - Customizable**
- Adicione a política `AmazonS3ReadOnlyAccess`
- Finalize a criação da role e copie o ARN.
- Salve o ARN pois irá ser utilizado quando for realizar o setup do ambiente local

### 5. Configurar Permissões no Bucket S3
- Acesse o bucket no S3 → Aba Permissões → Política do bucket.
- Adicione a política abaixo (substitua os valores adequadamente):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowRedshiftServerlessReadAccess",
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": [
        "arn:aws:s3:::final-data-game",
        "arn:aws:s3:::final-data-game/*"
      ],
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "YOUR_ACCOUNT_ID"
        },
        "ArnLike": {
          "aws:SourceArn": "YOUR_REDSHIFT_WORKGROUP_ARN"
        }
      }
    }
  ]
}

    """)

    st.markdown("---")
    st.subheader("🧩 Pronto para configurar o ambiente local?")
    if st.button("🖥️ Ir para Ambiente Local"):
        change_page("🖥️ Ambiente Local")
        st.rerun()


# Página: Ambiente Local
elif st.session_state.page == "🖥️ Ambiente Local":
    st.title("🖥️ Setup: Ambiente Local")

    st.markdown("""
    ### 📄 1. Clonar o Repositório
    ```git
    git clone https://github.com/IgorCanelo/ETL.git
    ```      
    ### 🔐 2. Configuração do `.env`
    Crie um arquivo chamado de .env na raiz do projeto e preencha com as credenciais necessárias para testes locais.
                
    ```dotenv
    AWS_ACCESS_KEY_ID=your_access_key_aws
    AWS_SECRET_ACCESS_KEY=your_secret_key_aws
    SAVE_TABLE_1_S3=s3a://final-data-game/first-time-login/
    SAVE_TABLE_2_S3=s3a://final-data-game/login-agg-date/
    HOST_REDSHIFT=your_redshift_host
    SCHEMA_REDSHIFT=your_schema
    LOGIN_REDSHIFT=your_login
    PASSWORD_REDSHIFT=your_password
    SLACK_WEBHOOK_URL=your_webhook_slack
    ```
                
    ### 🔐 3. Configuração dos arquivos localizados em `sql-scripts/copy_table`
    - Para os dois arquivos:
        - `COPY` - Se os buckets e pastas foram criados com os nomes sugeridos não é necessário atualizar
        - `FROM` - Cole o seu ARN obtido do RedShift
                

    ### 🐳 4. Build do Docker
    ```bash
    docker build -f Dockerfile.airflow -t data_pipeline_project .
    ```

    ### 🔄 5. Subir os containers com Docker Compose
    ```bash
    docker-compose up
    ```

    ### 🌐 6. Acessar o Airflow
    - Acesse em: [http://localhost:8080](http://localhost:8080)
                
    ### 🧪 7. (Opcional) Conectar via DBeaver

    Para facilitar a visualização e execução de queries no banco, recomenda-se usar o **DBeaver**:

    1. Faça o download e instale o DBeaver: [https://dbeaver.io/download/](https://dbeaver.io/download/)
    2. Crie duas conexões:
        - Uma para o **PostgreSQL local**:
            - Host: `localhost`
            - Porta: `5433`
            - Database: `game_data`
            - Usuário: `game`
            - Senha: `game123`
        - Outra para o **Redshift Serverless**:
            - Host: `HOST_REDSHIFT` (do `.env`)
            - Porta: `5439`
            - Database: conforme configurado no Redshift
            - Usuário/Senha: conforme definido no `.env`

    💡 *Essa etapa é opcional, mas útil para debug e análise dos dados carregados.*

    """)
    
    st.markdown("---")
    if st.button("🏗️ Ver Arquitetura sugerida para uma empresa de jogos mobile"):
        change_page("🏗️ Arquitetura de Dados")
        st.rerun()

# Página: Detalhes do Projeto

    

# Página: Arquitetura de Dados
elif st.session_state.page == "🏗️ Arquitetura de Dados":
    st.title("🏗️ Arquitetura sugerida de Dados")

    st.markdown("""
    ### Arquitetura de Dados

    A arquitetura foi desenhada para ser **escalável**, **modular** e de **fácil manutenção**.

    ---

    #### ✅ Prós

    - **Separação clara por camadas (Raw, Bronze, Silver, Gold):**  
      Facilita o tratamento e a evolução da qualidade dos dados em cada etapa, promovendo um pipeline mais confiável e organizado.

    - **Uso do Airflow para orquestração:**  
      Ferramenta robusta e amplamente adotada para agendamento e monitoramento de workflows. Permite automações complexas com boa visibilidade. É open-source e possui uma grande comunidade.

    - **Ingestão de dados com Airbyte:**  
      Oferece conectores prontos para diversas fontes, acelerando a ingestão com baixo esforço de configuração. Também é open-source, permitindo customizações.

    - **Armazenamento escalável no Data Lake (S3):**  
      O Amazon S3 proporciona escalabilidade, durabilidade e flexibilidade para dados em diferentes formatos e volumes.

    - **Redshift como Data Warehouse:**  
      Eficiente para análises em larga escala, com boa integração ao ecossistema AWS.

    - **Visualização com Power BI:**  
      Entrega insights de forma amigável ao usuário final, com integração fluida com o Redshift e outras fontes.

    - **Observabilidade com CloudWatch e Slack:**  
      Permite monitoramento centralizado e alertas automáticos, agilizando a detecção e resolução de falhas.

    ---

    #### ⚠️ Contras

    - **Gerenciamento de ferramentas auto-hospedadas (Airflow e Airbyte):**  
      Exigem operação e manutenção constantes, o que pode aumentar a complexidade operacional em cenários mais exigentes.

    - **Custo e uso eficiente de recursos:**  
      O uso de serviços como EC2 e Redshift pode gerar custos elevados se não houver governança. É necessário gerenciar o tempo de execução das instâncias para evitar gastos desnecessários.

    - **Curva de aprendizado:**  
      As ferramentas escolhidas são poderosas, mas possuem curva de aprendizado significativa, podendo demandar mais tempo na implementação e onboarding da equipe.

    - **Monitoramento e logging distribuído:**  
      Logs e métricas espalhados entre diferentes ferramentas (Airflow, Airbyte, CloudWatch) podem dificultar a centralização e o troubleshooting em pipelines complexos.
    """)

    # Comentei a linha abaixo pois a imagem pode não existir
    st.image("images/arquitetura.png", caption="Diagrama da Arquitetura", use_container_width=True)
    
    st.markdown("---")
    if st.button("📌 Voltar para Overview"):
        change_page("📌 Overview")
        st.rerun()