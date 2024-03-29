
# Core Python
import os, sys

import json

from google.cloud import storage

# GVCEH objectscl
sys.path.insert(0, "utils/")
import gcp_tools as gt

# GCP
from google.cloud import secretmanager

def get_gcpsecrets(project_id,
                   secret_id,
                   version_id="latest"):
    """
    Access a secret version in Google Cloud Secret Manager.

    Args:
        project_id: GCP project ID.
        secret_id: ID of the secret you want to access.
        version_id: Version of the secret (defaults to "latest").

    Returns:
        The secret value as a string.
    """
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = client.access_secret_version(request={"name": name})

    # Return the payload as a string
    # Note: response.payload.data is a bytes object, decode it to a string
    return response.payload.data.decode("UTF-8")


def authenticate_implicit_with_adc(project_id, creds_json):
    """
    When interacting with Google Cloud Client libraries, the library can auto-detect the
    credentials to use.

    // TODO(Developer):
    //  1. Before running this sample,
    //  set up ADC as described in https://cloud.google.com/docs/authentication/external/set-up-adc
    //  2. Replace the project variable.
    //  3. Make sure that the user account or service account that you are using
    //  has the required permissions. For this sample, you must have "storage.buckets.list".
    Args:
        project_id: The project id of your Google Cloud project.
    """

    # This snippet demonstrates how to list buckets.
    # *NOTE*: Replace the client created below with the client required for your application.
    # Note that the credentials are not specified when constructing the client.
    # Hence, the client library will look for credentials using ADC.

    creds_dict = json.loads(creds_json)
    credentials = storage.Client.from_service_account_info(creds_dict)

    storage_client = storage.Client(credentials=credentials, project=creds_dict["project_id"])


    # storage_client = storage.Client(project=project_id)
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)
    print("Listed all storage buckets.")


if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_gcpsecrets("npaicivitas", "GOOGLE_APPLICATION_CREDENTIALS",
                                                                     "1")

    print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    authenticate_implicit_with_adc(project_id="test", creds_json=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])