# Shared Astra DB connection helper.
# All modules that need a Cassandra session should call connect() from here
# rather than duplicating the connection logic.

import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dotenv import load_dotenv

load_dotenv()


def connect():
    """
    Open and return an Astra DB session using the standard env vars:
        ASTRA_SECURE_BUNDLE_PATH
        ASTRA_DB_APPLICATION_TOKEN
        ASTRA_KEYSPACE
    """
    cloud_config = {"secure_connect_bundle": os.environ["ASTRA_SECURE_BUNDLE_PATH"]}
    auth_provider = PlainTextAuthProvider("token", os.environ["ASTRA_DB_APPLICATION_TOKEN"])
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    return cluster.connect(os.environ["ASTRA_KEYSPACE"])
