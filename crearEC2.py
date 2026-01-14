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

response = client.run_instances(
                                ImageId="ami-07ff62358b87c7116",
                                InstanceType="t3.micro",
                                MinCount=1,
                                MaxCount=1,
                                KeyName="claves",
                                SecurityGroupIds=['sg-0d21f2e25aa648b88'],
                                TagSpecifications=[{
                                    'ResourceType': 'instance',
                                    'Tags': [{'Key': 'Name','Value': 'Mi-Servidor-Python'},]
                                }],
                            )
id = response['Instances'][0]['InstanceId']
print(f"Instancia creada: {id}")

print("Esperando a que la instancia esté en ejecución")
client.get_waiter('instance_running').wait(InstanceIds=[id])

print("Parar el ec2.")
response = client.stop_instances(InstanceIds=[id])
client.get_waiter('instance_stopped').wait(InstanceIds=[id])
print("Se ha parado correctamente.")

print("Eliminar la instancia creada.")
response = client.terminate_instances(InstanceIds=[id])

client.get_waiter('instance_terminated').wait(InstanceIds=[id])
print("Instancia eliminada.")