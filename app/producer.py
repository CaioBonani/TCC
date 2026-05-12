#!/usr/bin/env python3
"""
=============================================================
PRODUTOR KAFKA - Data Replay DATASUS (SIH/SUS)
=============================================================
Le linhas reais da tabela PostgreSQL public.aih, definida em
ddl_tabela.sql, e reemite os registros no Kafka em modo
"conta-gotas" para simular ingestao near real-time em uma
Arquitetura Lambda.

Uso:
    python producer.py \
        --bootstrap-server kafka:9092 \
        --db-host postgres --db-name datasus --db-user postgres \
        --batch-size 10 --interval 1.0
=============================================================
"""

import argparse
import importlib.util
import hashlib
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    KafkaProducer = None
    NoBrokersAvailable = Exception


AIH_COLUMNS: Tuple[str, ...] = (
    "ano_cmpt",
    "mes_cmpt",
    "dt_inter",
    "dt_saida",
    "cep",
    "munic_res",
    "munic_mov",
    "cgc_hosp",
    "cnes",
    "nasc",
    "sexo",
    "idade",
    "cod_idade",
    "nacional",
    "instru",
    "raca_cor",
    "etnia",
    "cbor",
    "morte",
    "uti_mes_to",
    "marca_uti",
    "val_uti",
    "proc_solic",
    "proc_rea",
    "val_sh",
    "val_sp",
    "n_aih",
    "val_tot",
    "infehosp",
    "ind_vdrl",
    "diag_princ",
    "diag_secun",
    "diagsec1",
    "diagsec2",
    "diagsec3",
    "diagsec4",
    "diagsec5",
    "diagsec6",
    "diagsec7",
    "diagsec8",
    "diagsec9",
    "cid_morte",
)

DEFAULT_ORDER_BY = ("ano_cmpt", "mes_cmpt", "dt_inter", "dt_saida", "n_aih")

IBGE_UF_PREFIX_TO_SIGLA = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CONFIG_PATH = Path(__file__).with_name("db_connection_config.py")


def json_default(value: Any) -> Any:
    """Serializa tipos vindos do banco que o json padrao nao conhece."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Tipo nao serializavel em JSON: {type(value)!r}")


def carregar_config_hardcoded() -> Dict[str, Dict[str, Any]]:
    """Carrega credenciais locais se app/db_connection_config.py existir."""
    if not CONFIG_PATH.exists():
        return {}

    spec = importlib.util.spec_from_file_location("db_connection_config", CONFIG_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Nao foi possivel carregar {CONFIG_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return {
        "database": getattr(module, "DATABASE", {}),
        "ssh_tunnel": getattr(module, "SSH_TUNNEL", {}),
    }


def config_get(config: Dict[str, Dict[str, Any]], section: str, key: str, default: Any = None) -> Any:
    return config.get(section, {}).get(key, default)


def criar_producer(bootstrap_server: str, max_retries: int = 30) -> KafkaProducer:
    """Cria o KafkaProducer com retry automatico."""
    if KafkaProducer is None:
        raise RuntimeError(
            "Dependencia ausente: instale kafka-python para enviar mensagens ao Kafka."
        )

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_server],
                value_serializer=lambda v: json.dumps(
                    v,
                    ensure_ascii=False,
                    default=json_default,
                ).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=16384,
                compression_type="gzip",
            )
            print(f"Conectado ao Kafka em {bootstrap_server}")
            return producer
        except NoBrokersAvailable:
            print(f"Tentativa {attempt}/{max_retries} - Kafka indisponivel. Aguardando 5s...")
            time.sleep(5)

    raise RuntimeError(f"Nao foi possivel conectar ao Kafka apos {max_retries} tentativas")


def criar_conexao_postgres(args: argparse.Namespace, host: Optional[str] = None, port: Optional[int] = None):
    """Abre conexao com PostgreSQL usando psycopg2 apenas quando necessario."""
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "Dependencia ausente: instale psycopg2-binary no ambiente do producer."
        ) from exc

    if args.db_dsn:
        return psycopg2.connect(args.db_dsn)

    return psycopg2.connect(
        host=host or args.db_host,
        port=port or args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        connect_timeout=args.db_connect_timeout,
    )


@contextmanager
def abrir_tunel_ssh(args: argparse.Namespace):
    if not args.ssh_tunnel:
        yield args.db_host, args.db_port
        return

    try:
        from sshtunnel import SSHTunnelForwarder
    except ImportError as exc:
        raise RuntimeError(
            "Dependencia ausente: instale sshtunnel para usar --ssh-tunnel."
        ) from exc

    ssh_kwargs: Dict[str, Any] = {
        "ssh_address_or_host": (args.ssh_host, args.ssh_port),
        "ssh_username": args.ssh_user,
        "remote_bind_address": (args.db_host, args.db_port),
        "local_bind_address": (args.ssh_local_host, args.ssh_local_port),
    }

    if args.ssh_password:
        ssh_kwargs["ssh_password"] = args.ssh_password
    if args.ssh_pkey:
        ssh_kwargs["ssh_pkey"] = args.ssh_pkey

    print(
        "Abrindo tunel SSH: "
        f"{args.ssh_user}@{args.ssh_host}:{args.ssh_port} -> {args.db_host}:{args.db_port}"
    )
    with SSHTunnelForwarder(**ssh_kwargs) as tunnel:
        yield tunnel.local_bind_host, tunnel.local_bind_port


def validar_identificador(valor: str, nome: str) -> str:
    if not IDENTIFIER_RE.fullmatch(valor):
        raise ValueError(f"{nome} invalido para SQL identifier: {valor!r}")
    return valor


def quote_identifier(valor: str) -> str:
    validar_identificador(valor, "identifier")
    return f'"{valor}"'


def tabela_qualificada(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def montar_order_by(order_by: str) -> str:
    itens = [item.strip() for item in order_by.split(",") if item.strip()]
    if not itens:
        itens = list(DEFAULT_ORDER_BY)

    expressoes = []
    for item in itens:
        direcao = "ASC"
        coluna = item
        if item.startswith("-"):
            coluna = item[1:].strip()
            direcao = "DESC"
        if coluna not in AIH_COLUMNS:
            raise ValueError(
                f"Coluna invalida em --order-by: {coluna!r}. "
                f"Use uma das colunas do DDL."
            )
        expressoes.append(f"{quote_identifier(coluna)} {direcao} NULLS LAST")

    return ", ".join(expressoes)


def montar_query_aih(args: argparse.Namespace) -> Tuple[str, List[Any]]:
    colunas = ", ".join(quote_identifier(coluna) for coluna in AIH_COLUMNS)
    query = f"SELECT {colunas} FROM {tabela_qualificada(args.db_schema, args.db_table)}"
    filtros: List[str] = []
    params: List[Any] = []

    if args.start_year is not None:
        filtros.append(f"{quote_identifier('ano_cmpt')} >= %s")
        params.append(args.start_year)
    if args.end_year is not None:
        filtros.append(f"{quote_identifier('ano_cmpt')} <= %s")
        params.append(args.end_year)
    if args.start_dt_inter:
        filtros.append(f"{quote_identifier('dt_inter')} >= %s")
        params.append(args.start_dt_inter)
    if args.end_dt_inter:
        filtros.append(f"{quote_identifier('dt_inter')} <= %s")
        params.append(args.end_dt_inter)

    if filtros:
        query += " WHERE " + " AND ".join(filtros)

    if not args.no_order:
        query += " ORDER BY " + montar_order_by(args.order_by)

    if args.limit > 0:
        query += " LIMIT %s"
        params.append(args.limit)
    elif args.total > 0:
        query += " LIMIT %s"
        params.append(args.total)

    return query, params


def normalizar_texto(valor: Any) -> Optional[str]:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def to_int(valor: Any) -> Optional[int]:
    if valor is None:
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def to_float(valor: Any) -> float:
    if valor is None:
        return 0.0
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def parse_data_datasus(valor: Any) -> Optional[datetime]:
    texto = normalizar_texto(valor)
    if not texto or len(texto) != 8 or not texto.isdigit() or texto == "00000000":
        return None
    try:
        return datetime.strptime(texto, "%Y%m%d")
    except ValueError:
        return None


def formatar_data_evento(valor: Any) -> Optional[str]:
    data = parse_data_datasus(valor)
    if data is None:
        return None
    return data.strftime("%Y-%m-%d 00:00:00")


def calcular_dias_permanencia(row: Dict[str, Any]) -> int:
    entrada = parse_data_datasus(row.get("dt_inter"))
    saida = parse_data_datasus(row.get("dt_saida"))
    if entrada is None or saida is None:
        return 0
    return max((saida - entrada).days, 0)


def inferir_uf(row: Dict[str, Any]) -> Optional[str]:
    for campo in ("munic_res", "munic_mov"):
        municipio = normalizar_texto(row.get(campo))
        if municipio and len(municipio) >= 2:
            uf = IBGE_UF_PREFIX_TO_SIGLA.get(municipio[:2])
            if uf:
                return uf
    return None


def normalizar_sexo(valor: Any) -> Optional[str]:
    codigo = to_int(valor)
    if codigo == 1:
        return "M"
    if codigo in (2, 3):
        return "F"
    return None


def idade_em_anos(row: Dict[str, Any]) -> Optional[int]:
    idade = to_int(row.get("idade"))
    cod_idade = to_int(row.get("cod_idade"))

    if idade is None:
        return None
    if cod_idade in (2, 3):
        return 0
    if cod_idade == 5:
        return max(idade, 100)
    return idade


def faixa_etaria(idade: Optional[int]) -> Optional[str]:
    if idade is None:
        return None
    if idade <= 4:
        return "0-4"
    if idade <= 9:
        return "5-9"
    if idade <= 14:
        return "10-14"
    if idade <= 19:
        return "15-19"
    if idade <= 29:
        return "20-29"
    if idade <= 39:
        return "30-39"
    if idade <= 49:
        return "40-49"
    if idade <= 59:
        return "50-59"
    if idade <= 69:
        return "60-69"
    if idade <= 79:
        return "70-79"
    return "80+"


def tem_uti(row: Dict[str, Any]) -> bool:
    return (
        (to_int(row.get("uti_mes_to")) or 0) > 0
        or (to_int(row.get("marca_uti")) or 0) > 0
        or to_float(row.get("val_uti")) > 0
    )


def linha_serializavel(row: Dict[str, Any]) -> Dict[str, Any]:
    serializada = {}
    for coluna in AIH_COLUMNS:
        valor = row.get(coluna)
        if isinstance(valor, Decimal):
            serializada[coluna] = float(valor)
        else:
            serializada[coluna] = valor
    return serializada


def gerar_id_estavel(row: Dict[str, Any]) -> str:
    payload = json.dumps(
        linha_serializavel(row),
        sort_keys=True,
        ensure_ascii=False,
        default=json_default,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def transformar_aih_em_evento(
    row: Dict[str, Any],
    *,
    sequence: int,
    cycle: int,
    source_table: str,
) -> Dict[str, Any]:
    idade = idade_em_anos(row)
    procedimento = normalizar_texto(row.get("proc_rea")) or normalizar_texto(row.get("proc_solic"))
    emitido_em = datetime.utcnow().isoformat(timespec="seconds")

    return {
        "id": gerar_id_estavel(row),
        "timestamp": emitido_em,
        "uf": inferir_uf(row),
        "municipio_codigo": normalizar_texto(row.get("munic_res"))
        or normalizar_texto(row.get("munic_mov")),
        "cnes": normalizar_texto(row.get("cnes")),
        "cid_principal": normalizar_texto(row.get("diag_princ")),
        "cid_descricao": None,
        "sexo": normalizar_sexo(row.get("sexo")),
        "faixa_etaria": faixa_etaria(idade),
        "idade": idade,
        "carater_internacao": None,
        "data_internacao": formatar_data_evento(row.get("dt_inter")),
        "data_alta": formatar_data_evento(row.get("dt_saida")),
        "dias_permanencia": calcular_dias_permanencia(row),
        "valor_aih": to_float(row.get("val_tot")),
        "obito": to_int(row.get("morte")) == 1,
        "uti": tem_uti(row),
        "procedimento_principal": procedimento,
        "source_table": source_table,
        "replay_sequence": sequence,
        "replay_cycle": cycle,
        "raw_aih": linha_serializavel(row),
    }


def buscar_lote(cursor, batch_size: int) -> List[Dict[str, Any]]:
    rows = cursor.fetchmany(batch_size)
    return [dict(row) for row in rows]


def resumo_datas(rows: Sequence[Dict[str, Any]]) -> str:
    datas = [normalizar_texto(row.get("dt_inter")) for row in rows if row.get("dt_inter")]
    if not datas:
        return "dt_inter sem valor"
    return f"dt_inter {min(datas)}..{max(datas)}"


def validar_datas_args(args: argparse.Namespace) -> None:
    for nome in ("start_dt_inter", "end_dt_inter"):
        valor = getattr(args, nome)
        if valor and (len(valor) != 8 or not valor.isdigit()):
            raise ValueError(f"--{nome.replace('_', '-')} deve estar no formato AAAAMMDD")


def criar_parser(config: Dict[str, Dict[str, Any]]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produtor Kafka - Data Replay DATASUS AIH")

    parser.add_argument("--bootstrap-server", default="kafka:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="datasus-internacoes", help="Topico Kafka")
    parser.add_argument("--interval", type=float, default=1.0, help="Pausa entre lotes do replay (segundos)")
    parser.add_argument("--batch-size", type=int, default=10, help="Mensagens por lote")
    parser.add_argument("--total", type=int, default=0, help="Total de mensagens a emitir (0 = toda a selecao)")
    parser.add_argument("--dry-run", action="store_true", help="Le do banco e transforma, mas nao envia ao Kafka")
    parser.add_argument("--print-events", action="store_true", help="Imprime cada evento JSON gerado")

    parser.add_argument("--db-dsn", default=config_get(config, "database", "dsn", os.getenv("POSTGRES_DSN")), help="DSN PostgreSQL completo")
    parser.add_argument("--db-host", default=config_get(config, "database", "host", os.getenv("POSTGRES_HOST", "postgres")), help="Host PostgreSQL visto a partir do servidor SSH ou da rede local")
    parser.add_argument("--db-port", type=int, default=int(config_get(config, "database", "port", os.getenv("POSTGRES_PORT", "5432"))), help="Porta PostgreSQL")
    parser.add_argument("--db-name", default=config_get(config, "database", "name", os.getenv("POSTGRES_DB", "datasus")), help="Database PostgreSQL")
    parser.add_argument("--db-user", default=config_get(config, "database", "user", os.getenv("POSTGRES_USER", "postgres")), help="Usuario PostgreSQL")
    parser.add_argument("--db-password", default=config_get(config, "database", "password", os.getenv("POSTGRES_PASSWORD")), help="Senha PostgreSQL")
    parser.add_argument("--db-schema", default=config_get(config, "database", "schema", os.getenv("POSTGRES_SCHEMA", "public")), help="Schema da tabela AIH")
    parser.add_argument("--db-table", default=config_get(config, "database", "table", os.getenv("POSTGRES_TABLE", "aih")), help="Tabela AIH")
    parser.add_argument(
        "--db-connect-timeout",
        type=int,
        default=int(config_get(config, "database", "connect_timeout", os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))),
        help="Timeout de conexao PostgreSQL em segundos",
    )
    parser.add_argument(
        "--ssh-tunnel",
        action="store_true",
        default=bool(config_get(config, "ssh_tunnel", "enabled", False)),
        help="Abre tunel SSH para acessar o PostgreSQL, como no DBeaver.",
    )
    parser.add_argument("--ssh-host", default=config_get(config, "ssh_tunnel", "host", os.getenv("SSH_TUNNEL_HOST")), help="Host SSH/bastion")
    parser.add_argument("--ssh-port", type=int, default=int(config_get(config, "ssh_tunnel", "port", os.getenv("SSH_TUNNEL_PORT", "22"))), help="Porta SSH")
    parser.add_argument("--ssh-user", default=config_get(config, "ssh_tunnel", "user", os.getenv("SSH_TUNNEL_USER")), help="Usuario SSH")
    parser.add_argument("--ssh-password", default=config_get(config, "ssh_tunnel", "password", os.getenv("SSH_TUNNEL_PASSWORD")), help="Senha SSH")
    parser.add_argument("--ssh-pkey", default=config_get(config, "ssh_tunnel", "pkey", os.getenv("SSH_TUNNEL_PKEY")), help="Caminho da chave privada SSH")
    parser.add_argument("--ssh-local-host", default=config_get(config, "ssh_tunnel", "local_host", "127.0.0.1"), help="Host local do tunel")
    parser.add_argument("--ssh-local-port", type=int, default=int(config_get(config, "ssh_tunnel", "local_port", "0")), help="Porta local do tunel; 0 escolhe automaticamente")

    parser.add_argument("--start-year", type=int, help="Filtra ano_cmpt >= valor")
    parser.add_argument("--end-year", type=int, help="Filtra ano_cmpt <= valor")
    parser.add_argument("--start-dt-inter", help="Filtra dt_inter >= AAAAMMDD")
    parser.add_argument("--end-dt-inter", help="Filtra dt_inter <= AAAAMMDD")
    parser.add_argument("--limit", type=int, default=0, help="Limite SQL por ciclo de replay (0 = sem limite)")
    parser.add_argument(
        "--order-by",
        default=",".join(DEFAULT_ORDER_BY),
        help="Colunas do DDL para ordenacao, separadas por virgula. Prefixe com '-' para DESC.",
    )
    parser.add_argument(
        "--no-order",
        action="store_true",
        help="Nao adiciona ORDER BY na consulta. Use para iniciar streaming rapidamente em tabelas grandes.",
    )
    parser.add_argument("--fetch-size", type=int, default=1000, help="Tamanho do fetch server-side no PostgreSQL")
    parser.add_argument("--loop", action="store_true", help="Ao chegar ao fim da selecao, reinicia o replay")
    parser.add_argument(
        "--loop-sleep",
        type=float,
        default=5.0,
        help="Pausa antes de reiniciar quando --loop estiver ativo",
    )

    return parser


def enviar_lote(
    *,
    producer: Optional[KafkaProducer],
    topic: str,
    rows: Sequence[Dict[str, Any]],
    sequence_start: int,
    cycle: int,
    source_table: str,
    dry_run: bool,
    print_events: bool,
) -> int:
    enviadas = 0
    for row in rows:
        sequence = sequence_start + enviadas + 1
        evento = transformar_aih_em_evento(
            row,
            sequence=sequence,
            cycle=cycle,
            source_table=source_table,
        )

        if print_events:
            print(json.dumps(evento, ensure_ascii=False, default=json_default))

        if not dry_run and producer is not None:
            producer.send(topic, key=evento["uf"], value=evento)

        enviadas += 1

    if not dry_run and producer is not None:
        producer.flush()

    return enviadas


def main() -> None:
    config = carregar_config_hardcoded()
    parser = criar_parser(config)
    args = parser.parse_args()
    validar_datas_args(args)
    validar_identificador(args.db_schema, "--db-schema")
    validar_identificador(args.db_table, "--db-table")
    if args.ssh_tunnel and (not args.ssh_host or not args.ssh_user):
        raise ValueError("--ssh-host e --ssh-user sao obrigatorios quando --ssh-tunnel esta ativo")

    if args.batch_size <= 0:
        raise ValueError("--batch-size deve ser maior que zero")
    if args.fetch_size <= 0:
        raise ValueError("--fetch-size deve ser maior que zero")
    if args.total < 0:
        raise ValueError("--total nao pode ser negativo")

    source_table = f"{args.db_schema}.{args.db_table}"
    query, params = montar_query_aih(args)

    print("=" * 60)
    print("PRODUTOR KAFKA - DATA REPLAY DATASUS AIH")
    print("=" * 60)
    print(f"  Broker:       {args.bootstrap_server}")
    print(f"  Topico:       {args.topic}")
    print(f"  Fonte:        PostgreSQL {source_table}")
    print(f"  Tunel SSH:    {'sim' if args.ssh_tunnel else 'nao'}")
    print(f"  Intervalo:    {args.interval}s")
    print(f"  Batch size:   {args.batch_size}")
    print(f"  Total:        {'toda a selecao' if args.total == 0 else args.total}")
    print(f"  Loop:         {'sim' if args.loop else 'nao'}")
    print(f"  Dry run:      {'sim' if args.dry_run else 'nao'}")
    print("=" * 60)

    producer = None if args.dry_run else criar_producer(args.bootstrap_server)
    total_enviadas = 0
    cycle = 0

    try:
        with abrir_tunel_ssh(args) as (db_host, db_port):
            while True:
                cycle += 1
                print(f"Iniciando ciclo de replay {cycle}: {query} | params={params}")

                conn = criar_conexao_postgres(args, host=db_host, port=db_port)
                conn.autocommit = False
                try:
                    from psycopg2.extras import RealDictCursor

                    cursor_name = f"aih_replay_{os.getpid()}_{cycle}"
                    cursor = conn.cursor(name=cursor_name, cursor_factory=RealDictCursor)
                    cursor.itersize = args.fetch_size
                    cursor.execute(query, params)

                    while True:
                        lote = buscar_lote(cursor, args.batch_size)
                        if not lote:
                            break

                        if args.total > 0:
                            restante = args.total - total_enviadas
                            if restante <= 0:
                                break
                            lote = lote[:restante]

                        enviadas = enviar_lote(
                            producer=producer,
                            topic=args.topic,
                            rows=lote,
                            sequence_start=total_enviadas,
                            cycle=cycle,
                            source_table=source_table,
                            dry_run=args.dry_run,
                            print_events=args.print_events,
                        )
                        total_enviadas += enviadas

                        modo = "Lidas" if args.dry_run else "Enviadas"
                        print(
                            f"[{datetime.now().strftime('%H:%M:%S')}] "
                            f"{modo}: {total_enviadas} mensagens | "
                            f"Ultimo lote: {enviadas} registros | {resumo_datas(lote)}"
                        )

                        if args.total > 0 and total_enviadas >= args.total:
                            print(f"Total de {args.total} mensagens atingido. Encerrando.")
                            return

                        time.sleep(args.interval)
                finally:
                    conn.close()

                if not args.loop:
                    print("Fim da selecao PostgreSQL. Replay concluido.")
                    return

                print(f"Fim da selecao. Reiniciando em {args.loop_sleep}s por causa de --loop.")
                time.sleep(args.loop_sleep)

    except KeyboardInterrupt:
        print(f"Interrompido pelo usuario. Total processado: {total_enviadas}")
    finally:
        if producer is not None:
            producer.close()
        print("Producer encerrado.")


if __name__ == "__main__":
    main()
