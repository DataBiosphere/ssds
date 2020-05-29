import os
from functools import lru_cache

import boto3
import botocore.session
from botocore import credentials

@lru_cache()
def resource(name):
    return get_session().resource(name)

@lru_cache()
def client(name):
    return get_session().client(name)

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
