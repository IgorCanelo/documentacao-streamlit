import streamlit as st
import pandas as pd

df_login_agg_date = pd.read_csv("tables/login_agg_date.csv", sep=",", index_col=0)
df_first_time_login = pd.read_csv(
    "tables/first_time_login.csv",
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
st.sidebar.title("Navigation")
nav = st.sidebar.radio("Go to:", ["📌 Overview", "☁️ Cloud Environment", "🖥️ Local Environment", 
                                  "🏗️ Data Architecture"],
                                  index=["📌 Overview", "☁️ Cloud Environment", "🖥️ Local Environment", 
                                  "🏗️ Data Architecture"].index(st.session_state.page))


# Atualiza a página ativa com base no radio button
if nav != st.session_state.page:
    st.session_state.page = nav
    st.rerun()

# Página: Overview
if st.session_state.page == "📌 Overview":


    st.title("📌 Overview")

    st.markdown("""
    This project implements a complete pipeline for simulating and analyzing user logins in a game. To test its execution, follow the setup steps for the local and cloud environments described in the following sections.

    After completing the setup, the DAG will be ready to run. In a production scenario, DAGs should be scheduled for daily execution, ensuring that the data is always up to date.

    The pipeline execution starts manually through the `dag_data_creation` DAG. The `dag_data_transformation` DAG was developed to start automatically after the first one finishes.

    The project was structured to ensure maximum automation and fault tolerance. For this, Slack notifications were implemented for both success and failure cases, sent to a specific logs channel. This approach provides operational visibility in environments with multiple DAGs, allowing for quick and effective responses to failures.

    Notification preview:
    """)

    st.image("images/logs_slack.png", caption="Slack Notifications - Airflow", use_container_width=True)

    st.markdown("---")

    st.markdown("""
    ### 🎯 Objective
    The main objective of the project was to provide analytical tables that can be used by analysts to generate relevant insights that support decision-making.

    The architecture was designed to be scalable, considering the continuous growth of the user base and data volume. Therefore, technologies such as PySpark and SparkSQL were chosen, as they allow efficient processing of large datasets.
    """)

    st.markdown("---")

    st.markdown("""
    ### 🧾 Final Tables Generated

    - **`login_agg_date`**  
      Table aggregated by date. Since the original login records are timestamp-based, a daily aggregation was performed, counting the number of logins per user each day. It also includes information about the continent, country, and operating system used.
    """)
    st.dataframe(df_login_agg_date)

    st.markdown("""
    - **`first_time_login`**  
      This table presents an aggregation by `user_id`, containing the total number of logins performed by each user, as well as the date and time of their first recorded login. Additionally, it includes information on continent, country, and operating system.
    """)
    st.dataframe(df_first_time_login)

    st.markdown("""
    Just to reinforce: in a production environment, it is essential that DAGs run daily to ensure continuous updates of the analytical tables.
    """)

    st.markdown("---")

    st.subheader("📁 Project Folder Structure")


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
    st.subheader("✅ Ready to get started?")
    if st.button("☁️ Set Up Cloud Environment"):
        change_page("☁️ Cloud Environment")
        st.rerun()

# Page: Cloud Environment
elif st.session_state.page == "☁️ Cloud Environment":
    st.title("☁️ Setup: Cloud Environment (AWS)")


    st.markdown("""
    ## ✍️ Introduction

    This document outlines the necessary steps to configure and run the project on AWS. The process involves three main aspects: establishing a local connection to Redshift Serverless, creating the S3 buckets, and setting their respective policies and security configurations to allow Redshift access to those buckets.

    These steps were adopted due to the decision to develop the code using PySpark. Initially, I used a direct connection to Redshift via the JDBC driver provided by AWS. While this approach allowed exclusive use of Redshift without requiring other AWS services, it has a significant limitation. Redshift Serverless is billed based on query and runtime, and using JDBC causes PySpark to send data row by row. This practice, in addition to being extremely inefficient in terms of performance, results in significantly higher costs due to the extended runtime required to save the data.

    Because of this, I chose a more efficient approach: saving the data in S3 in Parquet format—a lightweight and optimized format that allows PySpark to use distributed processing. Then, using the `COPY` command, the data is transferred to Redshift. This strategy offers two key advantages: Redshift is natively structured to perform the `COPY` using parallel processing, which ensures much better performance, and because it is faster, it significantly reduces Redshift's active time, directly contributing to cost savings.

    Therefore, the steps described below are intended to ensure greater efficiency, performance, and cost-effectiveness when running the project.

    ---

    ## ✅ Setup Step-by-Step

    ### 1. Create the Workgroup and Namespace in Redshift Serverless

    - Go to the Amazon Redshift console → Redshift Serverless.
    - Create a new **Workgroup** and a **Namespace**.
    - Attach an **IAM Role** with permissions to access S3 (detailed in step 4).
    - Go to Redshift → Workgroup → Edit settings.
    - Enable the **Publicly accessible** option.

    ### 2. Create the Bucket in Amazon S3

    - Access the Amazon S3 console and create a bucket (e.g., `final-data-game`).
    - Inside the bucket, create two folders (e.g., `login-agg-date` and `first-time-login`).

    ### 3. Configure the Security Group

    - Go to **EC2 → Security Groups**.
    - Locate the security group associated with your Redshift Workgroup.
    - Add an **inbound rule**:
        - Type: PostgreSQL
        - Protocol: TCP
        - Port: 5439
        - Source: My IP

    ### 4. Create and Configure the IAM Role

    - Go to **IAM → Roles → Create Role**.
    - Trusted entity type: **AWS Service**  
    - Service: **Redshift**  
    - Use case: **Redshift - Customizable**
    - Attach the policy `AmazonS3ReadOnlyAccess`
    - Finish creating the role and copy the ARN.
    - Save the ARN, as it will be used when setting up the local environment.

    ### 5. Configure Permissions in the S3 Bucket
    - Go to the S3 bucket → Permissions tab → Bucket Policy.
    - Add the following policy (replace values as appropriate):
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
    st.subheader("🧩 Ready to set up the local environment?")
    if st.button("🖥️ Go to Local Environment"):
        change_page("🖥️ Local Environment")
        st.rerun()



# Página: Ambiente Local
elif st.session_state.page == "🖥️ Local Environment":
    st.title("🖥️ Setup: Local Environment")

    st.markdown("""
    ### 📄 1. Clone the Repository
    ```git
    git clone https://github.com/IgorCanelo/ETL.git
    ```      

    ### 🔐 2. `.env` Configuration
    Create a file named `.env` at the root of the project and fill it with the necessary credentials for local testing.

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

    ### 🔐 3. Configure the Files in `sql-scripts/copy_table`
    - For both files:
        - `COPY` - If the buckets and folders were created with the suggested names, no update is needed.
        - `FROM` - Paste your ARN obtained from Redshift.
                
    ### 🐳 4. Build the Docker Image
    ```bash
    docker build -f Dockerfile.airflow -t data_pipeline_project .
    ```

    ### 🔄 5. Start the Containers with Docker Compose
    ```bash
    docker-compose up
    ```

    ### 🌐 6. Access Airflow
    - Open in your browser: [http://localhost:8080](http://localhost:8080)
                
    ### 🧪 7. (Optional) Connect via DBeaver

    To make it easier to visualize and run queries on the database, it is recommended to use **DBeaver**:

    1. Download and install DBeaver: [https://dbeaver.io/download/](https://dbeaver.io/download/)
    2. Create two connections:
        - One for **local PostgreSQL**:
            - Host: `localhost`
            - Port: `5433`
            - Database: `game_data`
            - User: `game`
            - Password: `game123`
        - One for **Redshift Serverless**:
            - Host: `HOST_REDSHIFT` (from `.env`)
            - Port: `5439`
            - Database: as configured in Redshift
            - User/Password: as defined in `.env`

    💡 *This step is optional, but useful for debugging and analyzing loaded data.*
    """)

    st.markdown("---")
    if st.button("🏗️ View Suggested Architecture for a Mobile Game Company"):
        change_page("🏗️ Data Architecture")
        st.rerun()


# Página: Detalhes do Projeto

    

# Página: Arquitetura de Dados
elif st.session_state.page == "🏗️ Data Architecture":
    st.title("🏗️ Suggested Data Architecture")

    st.markdown("""
    ### Data Architecture

    The architecture was designed to be **scalable**, **modular**, and **easy to maintain**.

    ---

    #### ✅ Pros

    - **Clear separation by layers (Raw, Bronze, Silver, Gold):**  
      Facilitates data processing and quality improvement at each stage, promoting a more reliable and organized pipeline.

    - **Use of Airflow for orchestration:**  
      A robust and widely adopted tool for scheduling and monitoring workflows. Supports complex automation with good visibility. It's open-source and has a large community.

    - **Data ingestion with Airbyte:**  
      Provides ready-made connectors for various sources, accelerating ingestion with minimal configuration effort. Also open-source and customizable.

    - **Scalable storage in the Data Lake (S3):**  
      Amazon S3 offers scalability, durability, and flexibility for handling data in various formats and volumes.

    - **Redshift as a Data Warehouse:**  
      Efficient for large-scale analytics, with strong integration within the AWS ecosystem.

    - **Visualization with Power BI:**  
      Delivers insights in a user-friendly way, with smooth integration with Redshift and other sources.

    - **Observability with CloudWatch and Slack:**  
      Enables centralized monitoring and automatic alerts, speeding up failure detection and resolution.

    ---

    #### ⚠️ Cons

    - **Management of self-hosted tools (Airflow and Airbyte):**  
      Requires continuous operation and maintenance, which can increase operational complexity in more demanding scenarios.

    - **Cost and efficient resource usage:**  
      Services like EC2 and Redshift can become expensive without proper governance. Instance run times must be managed to avoid unnecessary expenses.

    - **Learning curve:**  
      The chosen tools are powerful but have a significant learning curve, potentially requiring more time for implementation and team onboarding.

    - **Distributed monitoring and logging:**  
      Logs and metrics scattered across different tools (Airflow, Airbyte, CloudWatch) can make centralization and troubleshooting more challenging in complex pipelines.
    """)


    # Comentei a linha abaixo pois a imagem pode não existir
    st.image("images/arquitetura.png", caption="Architecture diagram", use_container_width=True)
    
    st.markdown("---")
    if st.button("📌 Back to Overview"):
        change_page("📌 Overview")
        st.rerun()