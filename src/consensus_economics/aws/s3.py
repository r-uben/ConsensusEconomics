import boto3
import pandas as pd
import json

# Note: This code requires proper AWS credentials configuration to work
# See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

class S3:

    def __init__(self) -> None:
        self.__buckets = None

    @property   
    def buckets(self):
        if self.__buckets is None:
            s3 = boto3.client('s3')
            buckets = s3.list_buckets().get('Buckets', [])
            self.__buckets = pd.DataFrame([bucket['Name'] for bucket in buckets], columns=['name'])
        return self.__buckets
    
    @property
    def secrets(self):
        client = boto3.client('secretsmanager')
        response = client.list_secrets()
        if 'SecretList' in response:
            secrets = [secret['Name'] for secret in response['SecretList']]
            return secrets
        else:
            return []
        
    def get_secret(self, secret_name):
        client = boto3.client('secretsmanager')
        try:
            response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return response['SecretString']
            else:
                return None
        except Exception as e:
            print(f"Error retrieving secret {secret_name}: {e}")
            return None

    def store_secret(self, secret_name, token, password, type="api"):
        client = boto3.client('secretsmanager')
        try:
            if type == "api":
                secret_value = {
                    'api_token': token,
                    'api_key': password
                }
            elif type == "password":
                secret_value = {
                    'username': token,
                    'password': password,
                }
            else:
                raise ValueError(f"Invalid type {type}")
            response = client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(secret_value)
            )
            return response['ARN']
        except client.exceptions.ResourceExistsException:
            print(f"Secret {secret_name} already exists. Use update_secret method to modify it.")
            return None
        except Exception as e:
            print(f"Error storing secret {secret_name}: {e}")
            return None
