# Crear un S3 Estándar, crear un cubo y añadir varias carpetas con un objeto que sea un archivo csv con varios datos para trabajar con él a posteriori y obtener le objeto
import boto3
from dotenv import load_dotenv
import os
import csv
import io
from datetime import datetime
load_dotenv()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

s3 = session.client('s3')

BUCKET = 's3negocioactividad'
REGION = os.getenv("REGION")
BASE_PREFIX = 'negocio'

def create_bucket_basic(bucket_name: str, region: str):
    """
    Crea el bucket si no existe.
    - En us-east-1 NO se pasa LocationConstraint.
    """
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket ya existe y es accesible: {bucket_name}")
        return
    except Exception:
        pass

    kwargs = {"Bucket": bucket_name}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3.create_bucket(**kwargs)
    print(f"Bucket creado: {bucket_name} (region={region})")


def upload_csv(bucket_name: str, key: str, headers: list[str], rows: list[list]):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow(r)

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8"
    )
    print(f"CSV subido: s3://{bucket_name}/{key}")


def read_csv_from_s3(bucket_name: str, key: str, max_rows: int = 5):
    """
    Descarga el CSV y muestra algunas filas (para trabajar luego con ello).
    """
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    text = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    print(f"\n[LECTURA] {key} (primeras {max_rows} filas):")
    for i, row in enumerate(reader):
        print(row)
        if i + 1 >= max_rows:
            break

def seed_and_upload(bucket_name: str):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Usuario
    usuario_h = ["id", "nombre", "correo", "telefono", "direccion", "fechaDeRegistro"]
    usuario_r = [
        [1, "Ana Pérez", "ana.perez@example.com", "+34-600-111-222", "C/ Sierpes 10, Sevilla", now],
        [2, "Luis García", "luis.garcia@example.com", "+34-600-333-444", "Av. Constitución 25, Sevilla", now],
        [3, "Marta Ruiz", "marta.ruiz@example.com", "", "C/ Betis 5, Sevilla", now],
    ]

    # Zona
    zona_h = ["id", "nombre", "categoria", "numeroIncidencias", "coordenadas"]
    zona_r = [
        [1, "Triana", "Urbana", 2, "37.3826,-6.0011"],
        [2, "Centro", "Urbana", 1, "37.3891,-5.9845"],
    ]

    # Reporte (FK id_usuario, id_zona)
    reporte_h = ["id", "id_usuario", "id_zona", "tipoIncidencia", "descripcion",
                 "fechaHora", "estado", "prioridad", "medioReporte", "ubicacion"]
    reporte_r = [
        [1, 1, 1, "Alumbrado", "Farola apagada desde ayer", now, "Abierto", "Media", "App", "C/ San Jacinto 12"],
        [2, 2, 1, "Basura", "Contenedor desbordado", now, "En progreso", "Alta", "Web", "Plaza del Altozano"],
        [3, 1, 2, "Baches", "Bache grande en calzada", now, "Abierto", "Alta", "Teléfono", "Av. de la Constitución"],
    ]

    # Comentario (FK id_usuario, id_reporte)
    comentario_h = ["id", "id_usuario", "id_reporte", "texto", "fecha"]
    comentario_r = [
        [1, 3, 1, "Yo también lo he visto, sigue apagada.", now],
        [2, 2, 3, "Pasa mucha gente por ahí, peligroso.", now],
    ]

    # Multimedia (FK id_reporte) -> normalmente guardarías aquí REFERENCIAS a objetos multimedia en S3
    multimedia_h = ["id", "id_reporte", "tipoArchivo", "rutaArchivo"]
    multimedia_r = [
        [1, 1, "image/jpeg", f"s3://{bucket_name}/{BASE_PREFIX}/multimedia/reporte=1/foto_1.jpg"],
        [2, 3, "video/mp4", f"s3://{bucket_name}/{BASE_PREFIX}/multimedia/reporte=3/video_1.mp4"],
    ]

    # Subimos a datos
    upload_csv(bucket_name, f"{BASE_PREFIX}/datos/usuario/usuario.csv", usuario_h, usuario_r)
    upload_csv(bucket_name, f"{BASE_PREFIX}/datos/zona/zona.csv", zona_h, zona_r)
    upload_csv(bucket_name, f"{BASE_PREFIX}/datos/reporte/reporte.csv", reporte_h, reporte_r)
    upload_csv(bucket_name, f"{BASE_PREFIX}/datos/comentario/comentario.csv", comentario_h, comentario_r)
    upload_csv(bucket_name, f"{BASE_PREFIX}/datos/multimedia/multimedia.csv", multimedia_h, multimedia_r)


def main():
    create_bucket_basic(BUCKET, REGION)

    prefixes = [
        f"{BASE_PREFIX}/datos/usuario/",
        f"{BASE_PREFIX}/datos/zona/",
        f"{BASE_PREFIX}/datos/reporte/",
        f"{BASE_PREFIX}/datos/comentario/",
        f"{BASE_PREFIX}/datos/multimedia/",
        f"{BASE_PREFIX}/multimedia/",
        f"{BASE_PREFIX}/resultados/",
    ]

    seed_and_upload(BUCKET)

    read_csv_from_s3(BUCKET, f"{BASE_PREFIX}/datos/reporte/reporte.csv", max_rows=5)

    print("Script básico finalizado.")


if __name__ == "__main__":
    main()