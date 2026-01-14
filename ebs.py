import boto3
from dotenv import load_dotenv
import os

load_dotenv()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))

client = session.client('ec2')

clave = "/home/alumnadotarde/Documentos/claves/claves.pem"
txt = "prueba.txt" 

instancia = client.run_instances(
                                ImageId="ami-07ff62358b87c7116",
                                InstanceType="t3.micro",
                                MinCount=1,
                                MaxCount=1,
                                KeyName="claves",
                                SecurityGroupIds=['sg-0d21f2e25aa648b88'],
                                Placement={'AvailabilityZone': 'us-east-1a'},
                                TagSpecifications=[{
                                    'ResourceType': 'instance',
                                    'Tags': [{'Key': 'Name','Value': 'Mi-Servidor-Python'},]
                                }],
                            )
id = instancia['Instances'][0]['InstanceId']
zona_disponibilidad = instancia['Instances'][0]['Placement']['AvailabilityZone']
client.get_waiter('instance_running').wait(InstanceIds=[id])

volumen = client.create_volume(
    AvailabilityZone=zona_disponibilidad,
    Size=1,
    VolumeType='gp3',
    TagSpecifications=[{'ResourceType':'volume','Tags':[{'Key':'Name','Value':'volumenPrueba'}]}]
)
volumeId = volumen['VolumeId']
print(f"Volumen creado: {volumeId}")

client.get_waiter('volume_available').wait(VolumeIds=[volumeId])

client.attach_volume(Device="/dev/sdf",InstanceId=id,VolumeId=volumeId)

desc = client.describe_instances(InstanceIds=[id])
public_ip = desc['Reservations'][0]['Instances'][0]['PublicIpAddress']

os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mkfs -t ext4 /dev/sdf"')
os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mkdir -p /mnt/volumen"')
os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mount /dev/sdf /mnt/volumen"')

os.system(f'scp -i {clave} {txt} ec2-user@{public_ip}:/tmp/{txt}')
os.system(f'ssh -i {clave} ec2-user@{public_ip} "sudo mv /tmp/{txt} /mnt/volumen/{txt}"')


print("Archivo prueba.txt copiado al volumen EBS en la instancia.")