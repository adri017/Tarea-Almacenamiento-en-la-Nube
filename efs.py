import boto3
from dotenv import load_dotenv
import os
import time

load_dotenv()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

ec2 = session.client("ec2")
efs = session.client("efs")

clave = "/home/alumnadotarde/Documentos/claves/claves.pem"
txt = "prueba.txt"
security_group_ec2 = "sg-0d21f2e25aa648b88"
security_group_efs = "sg-0d21f2e25aa648b88"
subnet_id = "subnet-037805d4f9f153e2e"
instancia = ec2.run_instances(
    ImageId="ami-07ff62358b87c7116",
    InstanceType="t3.micro",
    MinCount=1,
    MaxCount=1,
    KeyName="claves",
    SecurityGroupIds=[security_group_ec2]
)
instance_id = instancia["Instances"][0]["InstanceId"]
ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])

desc = ec2.describe_instances(InstanceIds=[instance_id])
public_ip = desc["Reservations"][0]["Instances"][0]["PublicIpAddress"]

efs_response = efs.create_file_system(PerformanceMode="generalPurpose", Encrypted=False)
efs_id = efs_response["FileSystemId"]

while True:
    fs = efs.describe_file_systems(FileSystemId=efs_id)
    if fs["FileSystems"][0]["LifeCycleState"] == "available":
        break
    time.sleep(5)

efs.create_mount_target(FileSystemId=efs_id, SubnetId=subnet_id, SecurityGroups=[security_group_efs])

while True:
    mts = efs.describe_mount_targets(FileSystemId=efs_id)["MountTargets"]
    if mts and all(mt["LifeCycleState"] == "available" for mt in mts):
        break
    time.sleep(5)

os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo yum install -y amazon-efs-utils"')
os.system(f'ssh -i {clave} ec2-user@{public_ip} "sudo mkdir -p /mnt/efs"')

os.system(f'ssh -i {clave} ec2-user@{public_ip} "sudo mount -t efs {efs_id}:/ /mnt/efs"')

resultado = os.system(f'ssh -i {clave} ec2-user@{public_ip} "mount | grep efs"')
if resultado != 0:
    raise Exception("EFS no se pudo montar en la EC2")

os.system(f'scp -i {clave} {txt} ec2-user@{public_ip}:/tmp/{txt}')
os.system(f'ssh -i {clave} ec2-user@{public_ip} "sudo mv /tmp/{txt} /mnt/efs/{txt}"')

print("Archivo copiado correctamente al EFS")
