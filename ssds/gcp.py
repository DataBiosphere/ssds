import os
import warnings
import requests
from functools import lru_cache
from typing import Optional
from requests.adapters import HTTPAdapter, DEFAULT_POOLSIZE

import google.auth
import google.auth.transport.requests
from google.cloud.storage import Client

from ssds.concurrency import MAX_RPC_CONCURRENCY, MAX_PASSTHROUGH_CONCURRENCY


@lru_cache()
def storage_client() -> Client:
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    client = Client()
    total_concurrency = MAX_RPC_CONCURRENCY + MAX_PASSTHROUGH_CONCURRENCY
    adapter = HTTPAdapter(pool_connections=total_concurrency, pool_maxsize=total_concurrency)
    client._http.mount("http://", adapter)
    client._http.mount("https://", adapter)
    return client

def resolve_billing_project(billing_project: Optional[str]=None) -> Optional[str]:
    if billing_project is not None:
        return billing_project
    elif os.environ.get('GOOGLE_PROJECT'):
        return os.environ['GOOGLE_PROJECT']
    elif os.environ.get('GCLOUD_PROJECT'):
        return os.environ['GCLOUD_PROJECT']
    elif os.environ.get('GCP_PROJECT'):
        return os.environ['GCP_PROJECT']
    else:
        return None

@lru_cache()
def get_identity():
    credentials, project_id = google.auth.default()
    try:
        credentials.refresh(google.auth.transport.requests.Request())
        resp = requests.get(f"https://www.googleapis.com/oauth2/v1/tokeninfo?id_token={credentials.id_token}")
        resp.raise_for_status()
        return resp.json()['email']
    except google.auth.exceptions.RefreshError:
        return credentials.service_account_email
