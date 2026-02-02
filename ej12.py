import os
import time
import boto3
import csv
import io
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

REGION = os.getenv("REGION")

BUCKET = "s3negocioactividad"
BASE_PREFIX = "negocio/particiones/reporte/"

DATABASE = "negocio_part_db"
TABLE = "reporte_part"

RESULTS_LOCATION = f"s3://{BUCKET}/resultados_athena_particiones/"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

s3 = session.client("s3")
athena = session.client("athena")


def put_csv(bucket: str, key: str, headers: list, rows: list):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    w.writerows(rows)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8"
    )
    print(f"CSV subido: s3://{bucket}/{key}")


def seed_partitioned_data():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["id", "id_usuario", "tipoIncidencia", "descripcion",
               "fechaHora", "estado", "prioridad", "medioReporte", "ubicacion"]
    zona_1 = [
        [1, 1, "Alumbrado", "Farola apagada desde ayer", now, "Abierto", "Media", "App", "C/ San Jacinto 12"],
        [2, 2, "Basura", "Contenedor desbordado", now, "En progreso", "Alta", "Web", "Plaza del Altozano"],
    ]
    zona_2 = [
        [3, 1, "Baches", "Bache grande en calzada", now, "Abierto", "Alta", "Teléfono", "Av. de la Constitución"],
    ]
    key1 = f"{BASE_PREFIX}id_zona=1/reporte.csv"
    key2 = f"{BASE_PREFIX}id_zona=2/reporte.csv"

    put_csv(BUCKET, key1, headers, zona_1)
    put_csv(BUCKET, key2, headers, zona_2)


def run_query(query: str, show_results: bool = False):
    q = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )
    qid = q["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(1)

    print(f"\n[QUERY {state}] {query.strip()}")

    if state == "SUCCEEDED" and show_results:
        res = athena.get_query_results(QueryExecutionId=qid)
        print("Resultados")
        for row in res["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            print(values)

    return qid

def main():
    seed_partitioned_data()
    run_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    location = f"s3://{BUCKET}/{BASE_PREFIX}"
    run_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
        id INT,
        id_usuario INT,
        tipoIncidencia STRING,
        descripcion STRING,
        fechaHora STRING,
        estado STRING,
        prioridad STRING,
        medioReporte STRING,
        ubicacion STRING
    )
    PARTITIONED BY (id_zona INT)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '\"'
    )
    LOCATION '{location}'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """)
    run_query(f"MSCK REPAIR TABLE {TABLE}")
    run_query(
        f"SELECT id, tipoIncidencia, prioridad FROM {TABLE} WHERE id_zona = 1",
        show_results=True
    )

if __name__ == "__main__":
    main()
