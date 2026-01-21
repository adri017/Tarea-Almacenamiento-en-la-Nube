import boto3
from dotenv import load_dotenv
import os
import time

load_dotenv()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))

ec2 = session.client('ec2')
efs = session.client('efs')

clave = "/home/alumnadotarde/Documentos/claves/claves.pem"
txt = "prueba.txt"
security_group = "sg-0d21f2e25aa648b88"
subnet_id = "subnet-079a42116ab27f89e"

instancia = ec2.run_instances(
                                ImageId="ami-07ff62358b87c7116",
                                InstanceType="t3.micro",
                                MinCount=1,
                                MaxCount=1,
                                KeyName="claves",
                                SecurityGroupIds=[security_group],
                                Placement={'AvailabilityZone': 'us-east-1a'},
                                TagSpecifications=[{
                                    'ResourceType': 'instance',
                                    'Tags': [{'Key': 'Name','Value': 'ec2-efs'},]
                                }],
                            )
instance_id = instancia['Instances'][0]['InstanceId']
ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])

desc = ec2.describe_instances(InstanceIds=[instance_id])
public_ip = desc['Reservations'][0]['Instances'][0]['PublicIpAddress']

print("EC2 creada:", instance_id)

efs_response = efs.create_file_system(
    PerformanceMode='generalPurpose',
    Encrypted=False,
    Tags=[{'Key': 'Name', 'Value': 'EFS-Prueba'}]
)

efs_id = efs_response['FileSystemId']
print("EFS creado:", efs_id)

time.sleep(10)

efs.create_mount_target(
    FileSystemId=efs_id,
    SubnetId=subnet_id,
    SecurityGroups=[security_group]
)

time.sleep(20)

os.system(
    f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} '
    f'"sudo yum install -y amazon-efs-utils"'
)

os.system(
    f'ssh -i {clave} ec2-user@{public_ip} '
    f'"sudo mkdir -p /mnt/efs"'
)

os.system(
    f'ssh -i {clave} ec2-user@{public_ip} '
    f'"sudo mount -t efs {efs_id}:/ /mnt/efs"'
)
os.system(
    f'scp -i {clave} {txt} ec2-user@{public_ip}:/tmp/{txt}'
)

os.system(
    f'ssh -i {clave} ec2-user@{public_ip} '
    f'"sudo mv /tmp/{txt} /mnt/efs/{txt}"'
)

print("Archivo copiado al EFS correctamente")
