import os
import boto3
from dotenv import load_dotenv

load_dotenv()
REGION = os.getenv("REGION")
BUCKET = 's3negocioactividadversionado'
KEY = "versionado/mensaje.txt"

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=REGION
)

s3 = session.client("s3")

def create_bucket_and_enable_versioning(bucket: str, region: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        args = {"Bucket": bucket}
        if region != "us-east-1":
            args["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**args)
        print(f"Bucket creado: {bucket}")

    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print("Versionado habilitado")

def upload_versions(bucket: str, key: str):
    # Versión 1
    v1 = "Versión 1 del archivo\n"
    r1 = s3.put_object(Bucket=bucket, Key=key, Body=v1.encode("utf-8"))
    v1_id = r1["VersionId"]
    print(f"Subida versión 1 (VersionId={v1_id})")

    # Versión 2 
    v2 = "Versión 2 del archivo (modificada)\n"
    r2 = s3.put_object(Bucket=bucket, Key=key, Body=v2.encode("utf-8"))
    v2_id = r2["VersionId"]
    print(f"Subida versión 2 (VersionId={v2_id})")

    return v1_id, v2_id

def show_versions(bucket: str, key: str):
    res = s3.list_object_versions(Bucket=bucket, Prefix=key)

    print("Version del objeto")
    for v in res.get("Versions", []):
        print(f"- VersionId={v['VersionId']} | IsLatest={v['IsLatest']}")

def get_specific_version(bucket: str, key: str, version_id: str):
    obj = s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
    content = obj["Body"].read().decode("utf-8")

    print(f"Contenido VersionId={version_id}]")
    print(content)

def main():
    create_bucket_and_enable_versioning(BUCKET, REGION)
    v1_id, v2_id = upload_versions(BUCKET, KEY)
    show_versions(BUCKET, KEY)
    get_specific_version(BUCKET, KEY, v1_id)
    get_specific_version(BUCKET, KEY, v2_id)

if __name__ == "__main__":
    main()