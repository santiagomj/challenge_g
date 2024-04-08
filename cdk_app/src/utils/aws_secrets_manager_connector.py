import json
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from utils.stdout_logger import Logger

logger = Logger("SecretsManagerConnector").get_logger()


def get_secret(secret_name: str, region_name: str = "us-east-1") -> Dict[str, Any]:
    """
    Retrieves a secret from AWS Secrets Manager.

    This function creates a session using boto3, then uses that session to create a client
    for the AWS Secrets Manager service. It attempts to retrieve the secret value associated
    with the provided `secret_name` in the specified `region_name`. If successful, it returns
    the secret value as a dictionary. If the retrieval fails due to a `ClientError`, the exception
    is raised.

    :param secret_name: The name of the secret to retrieve.
    :type secret_name: str
    :param region_name: The name of the AWS region where the secret is stored.
    :type region_name: str
    :return: The secret value as a dictionary.
    :rtype: Dict[str, Any]
    :raises ClientError: If any error occurs during the secret retrieval process.

    Usage Example:
        secret_info = get_secret("mySecretName", "us-west-2")
        print(secret_info)  # Prints the secret information as a dictionary

    Note:
        - It's essential to handle `ClientError` in the calling code to manage exceptions like
          AccessDeniedException, ResourceNotFoundException, etc.
        - The AWS credentials must have the appropriate permissions to access the specified secret.
    """
    logger.info(f"Attempting to retrieve secret: {secret_name} in region: {region_name}")

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        logger.info(f"Secret retrieved successfully: {secret_name}")

    except ClientError as e:
        logger.error(f"Failed to retrieve secret: {secret_name}. Error: {e}")
        raise e

    secret = get_secret_value_response["SecretString"]

    return json.loads(secret)
