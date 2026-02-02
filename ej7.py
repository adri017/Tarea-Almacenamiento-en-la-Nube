import os
import time
import boto3
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

REGION = os.getenv("REGION")
BUCKET = 's3negocioactividad'
KEY = "glacier/mensaje.txt"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

s3 = session.client("s3")

def create_bucket(bucket: str, region: str):
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except Exception as e:
        print('Error: ', e)

    args = {"Bucket": bucket}
    if region != "us-east-1":
        args["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3.create_bucket(**args)
    print(f"Bucket creado: {bucket}")


def upload_txt_glacier(bucket: str, key: str):
    msg = 'Este es el mensaje de prueba del ejercicio 7'

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=msg.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
        StorageClass="GLACIER"
    )
    print(f"Objeto subido en GLACIER: s3://{bucket}/{key}")


def restore_object(bucket: str, key: str):
    s3.restore_object(
        Bucket=bucket,
        Key=key,
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {"Tier": "Expedited"}
        }
    )
    print("Restauración solicitada")

def wait_until_restored(bucket: str, key: str, wait_seconds: int = 30):
    print("Esperando a que termine la restauración...")

    while True:
        meta = s3.head_object(Bucket=bucket, Key=key)
        restore = meta.get("Restore")

        if restore and 'ongoing-request="false"' in restore:
            print("Restauración completada")
            return
        print("  - Restauración en curso...")
        time.sleep(wait_seconds)

def get_and_print(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")

    print("Texto del ejercicio 7")
    print(text)

def main():
    create_bucket(BUCKET, REGION)
    upload_txt_glacier(BUCKET, KEY)
    restore_object(BUCKET, KEY)
    wait_until_restored(BUCKET, KEY)
    get_and_print(BUCKET, KEY)

if __name__ == "__main__":
    main()