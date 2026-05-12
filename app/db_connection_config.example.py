DATABASE = {
    "host": "host_do_postgres_visto_pelo_ssh",
    "port": 5432,
    "name": "datasus",
    "user": "postgres",
    "password": "senha_do_postgres",
    "schema": "public",
    "table": "aih",
    "connect_timeout": 10,
}

SSH_TUNNEL = {
    "enabled": True,
    "host": "host_ssh_ou_bastion",
    "port": 22,
    "user": "usuario_ssh",
    "password": "senha_ssh",
    "pkey": None,
    "local_host": "127.0.0.1",
    "local_port": 0,
}
