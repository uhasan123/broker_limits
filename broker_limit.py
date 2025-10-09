import os
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import psycopg2

class broker_limit:
    def __init__(self, config_path):
        load_dotenv(config_path)
        self.ssh_host = os.getenv("ssh_host")
        self.ssh_user = os.getenv("ssh_user")
        self.ssh_key = os.getenv("ssh_key")
        self.db_host = os.getenv("db_host")
        self.db_port = int(os.getenv("db_port"))
        self.db_name = os.getenv("db_name")
        self.db_user = os.getenv("db_user")
        self.db_password = os.getenv("db_password")

    def make_db_connection(self):
        # Open SSH tunnel
        tunnel = SSHTunnelForwarder(
            (self.ssh_host, 22),
            ssh_username=self.ssh_user,
            ssh_pkey=self.ssh_key,
            remote_bind_address=(self.db_host, self.db_port)
        )
        
        tunnel.start()
        # Connect to DB through the tunnel
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )
        return conn