import os
import time
import json
import boto3
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

REGION = os.getenv("REGION")
DATA_BUCKET = "s3negocioactividad"
JSON_PREFIX = "negocio/json/reporte/"
JSON_KEY = JSON_PREFIX + "reporte.json"
DATABASE = "negocio_json_db"
TABLE = "reporte_json"
RESULTS_BUCKET = DATA_BUCKET
RESULTS_LOCATION = f"s3://{RESULTS_BUCKET}/resultados_athena_json/"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

s3 = session.client("s3")
athena = session.client("athena")

def upload_json_to_s3():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        {
            "id": 1,
            "id_usuario": 1,
            "id_zona": 1,
            "tipoIncidencia": "Alumbrado",
            "descripcion": "Farola apagada desde ayer",
            "fechaHora": now,
            "estado": "Abierto",
            "prioridad": "Media",
            "medioReporte": "App",
            "ubicacion": "C/ San Jacinto 12"
        },
        {
            "id": 2,
            "id_usuario": 2,
            "id_zona": 1,
            "tipoIncidencia": "Basura",
            "descripcion": "Contenedor desbordado",
            "fechaHora": now,
            "estado": "En progreso",
            "prioridad": "Alta",
            "medioReporte": "Web",
            "ubicacion": "Plaza del Altozano"
        },
        {
            "id": 3,
            "id_usuario": 1,
            "id_zona": 2,
            "tipoIncidencia": "Baches",
            "descripcion": "Bache grande en calzada",
            "fechaHora": now,
            "estado": "Abierto",
            "prioridad": "Alta",
            "medioReporte": "Teléfono",
            "ubicacion": "Av. de la Constitución"
        }
    ]

    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"

    s3.put_object(
        Bucket=DATA_BUCKET,
        Key=JSON_KEY,
        Body=body.encode("utf-8"),
        ContentType="application/json; charset=utf-8"
    )

    print(f"Json subido: s3://{DATA_BUCKET}/{JSON_KEY}")

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

    print(f"[QUERY {state}] {query.strip()}")

    if state == "SUCCEEDED" and show_results:
        res = athena.get_query_results(QueryExecutionId=qid)
        print("Resultados:")
        for row in res["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            print(values)

    return qid

def main():
    upload_json_to_s3()
    run_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    json_location = f"s3://{DATA_BUCKET}/{JSON_PREFIX}"
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
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION '{json_location}'
    """)
    run_query(f"SELECT * FROM {TABLE}", show_results=True)
    run_query(
        f"SELECT id, tipoIncidencia, prioridad FROM {TABLE} WHERE prioridad = 'Alta'",
        show_results=True
    )
    run_query(
        f"SELECT id_zona, COUNT(*) AS total FROM {TABLE} GROUP BY id_zona",
        show_results=True
    )

if __name__ == "__main__":
    main()