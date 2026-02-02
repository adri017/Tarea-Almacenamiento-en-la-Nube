import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

REGION = os.getenv("REGION")
BUCKET = 's3negocioactividad'
KEY = "it/mensaje.txt"

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
        print(f"Bucket ya existe: {bucket}")
        return
    except ClientError as e:
        print('Error: ', e)

    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3.create_bucket(**kwargs)
    print(f"Bucket creado: {bucket}")

def upload_txt_intelligent_tiering(bucket: str, key: str):
    msg = 'Este es el mensaje de prueba del ejercicio 6'

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=msg.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
        StorageClass="INTELLIGENT_TIERING"
    )
    print(f"TXT subido en INTELLIGENT_TIERING: s3://{bucket}/{key}")

def get_and_print(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")

    print("Texto del objeto: ")
    print(text)

def main():
    create_bucket(BUCKET, REGION)
    upload_txt_intelligent_tiering(BUCKET, KEY)
    get_and_print(BUCKET, KEY)

if __name__ == "__main__":
    main()
