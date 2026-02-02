import os
import time
import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("REGION")
DATABASE = "negocio_db"
TABLE = "reporte"
DATA_LOCATION = "s3://s3negocioactividad/negocio/datos/reporte/"
RESULTS_BUCKET = 's3negocioactividad'
RESULTS_LOCATION = f"s3://{RESULTS_BUCKET}/resultados_athena/"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

athena = session.client("athena")
s3 = session.client("s3")

def create_results_bucket(bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except Exception as e:
        print('Error: ', e)

    args = {"Bucket": bucket}
    if REGION != "us-east-1":
        args["CreateBucketConfiguration"] = {"LocationConstraint": REGION}

    s3.create_bucket(**args)
    print(f"Bucket resultados Athena creado: {bucket}")

def run_query(query: str, show_results: bool = False):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )
    qid = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(1)

    print(f"[QUERY {state}] {query.strip()}")

    if state == "SUCCEEDED" and show_results:
        results = athena.get_query_results(QueryExecutionId=qid)

        print("Resultados: ")
        for row in results["ResultSet"]["Rows"]:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            print(values)

def main():
    create_results_bucket(RESULTS_BUCKET)

    run_query(f"""
    CREATE DATABASE IF NOT EXISTS {DATABASE}
    """)

    run_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
        id INT,
        id_usuario INT,
        id_zona INT,
        tipoIncidencia STRING,
        descripcion STRING,
        fechaHora STRING,
        estado STRING,
        prioridad STRING,
        medioReporte STRING,
        ubicacion STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '\"'
    )
    LOCATION '{DATA_LOCATION}'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """)

    run_query(f"""
    SELECT * FROM {TABLE}
    """, show_results=True)

    run_query(f"""
    SELECT id, tipoIncidencia, prioridad
    FROM {TABLE}
    WHERE prioridad = 'Alta'
    """, show_results=True)

    run_query(f"""
    SELECT id_zona, COUNT(*) AS total_incidencias
    FROM {TABLE}
    GROUP BY id_zona
    """, show_results=True)

if __name__ == "__main__":
    main()
