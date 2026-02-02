import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("REGION")
BUCKET = 's3negocioactividad'
KEY = "ia/mensaje.txt"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

s3 = session.client("s3")

def create_bucket_if_needed(bucket: str, region: str):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket ya existe y es accesible: {bucket}")
        return
    except ClientError as e:
        print('Error: ', e)

    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3.create_bucket(**kwargs)
    print(f"Bucket creado: {bucket} (region={region})")

def upload_txt_standard_ia(bucket: str, key: str):

    msg = 'Prueba de mensaje del ejercicio 5'

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=msg.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
        StorageClass="STANDARD_IA"
    )
    print(f"TXT subido en STANDARD_IA: s3://{bucket}/{key}")

def verify_storage_class(bucket: str, key: str):
    meta = s3.head_object(Bucket=bucket, Key=key)
    storage_class = meta.get("StorageClass", "STANDARD")
    print(f"StorageClass del objeto: {storage_class}")

def get_and_print(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")

    print('Texto del contenido del objeto: ')
    print(text)

def main():
    create_bucket_if_needed(BUCKET, REGION)
    upload_txt_standard_ia(BUCKET, KEY)
    verify_storage_class(BUCKET, KEY)
    get_and_print(BUCKET, KEY)

if __name__ == "__main__":
    main()