import os
import warnings
from functools import lru_cache
from typing import Optional
from requests.adapters import HTTPAdapter, DEFAULT_POOLSIZE

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
