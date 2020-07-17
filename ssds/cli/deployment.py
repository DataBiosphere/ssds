"""
Discover deployments of the SSDS
"""
import os
import argparse

import ssds
from ssds.deployment import Staging, Release
from ssds.cli import dispatch


deployment_cli = dispatch.group("deployment", help=__doc__)

@deployment_cli.command("list-staging")
def list_staging(args: argparse.Namespace):
    """
    List staging deployments of the SSDS
    """
    _list_deployments(Staging)

@deployment_cli.command("list-release")
def list_release(args: argparse.Namespace):
    """
    List release deployments of the SSDS
    """
    _list_deployments(Release)

def _list_deployments(deployments):
    for deployment in deployments:
        bucket = f"{deployment.ssds.blobstore.schema}{deployment.ssds.bucket}"
        print(deployment.name, bucket)
