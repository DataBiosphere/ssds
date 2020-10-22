import os
from functools import lru_cache

import boto3
import botocore.session
from botocore import config, credentials

from ssds.concurrency import MAX_RPC_CONCURRENCY, MAX_PASSTHROUGH_CONCURRENCY


@lru_cache()
def resource(name):
    return get_session().resource(name, config=_boto_config())

@lru_cache()
def client(name):
    return get_session().client(name, config=_boto_config())

@lru_cache()
def get_session():
    """
    Return a botocore session sharing awscli session caching
    """
    # This little dance is explained here:
    #   https://github.com/boto/botocore/pull/1157#issuecomment-387580482
    working_dir = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')
    session = botocore.session.get_session()
    provider = session.get_component('credential_provider').get_provider('assume-role')
    provider.cache = credentials.JSONFileCache(working_dir)
    return boto3.Session(botocore_session=session)

def _boto_config():
    return config.Config(max_pool_connections=MAX_RPC_CONCURRENCY + MAX_PASSTHROUGH_CONCURRENCY)

@lru_cache()
def get_identity():
    return client("sts").get_caller_identity()['UserId']
