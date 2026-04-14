#!/usr/bin/env python3
"""
=============================================================
PRODUTOR KAFKA - Simulação de Dados DATASUS (SIH/SUS)
=============================================================
Simula a ingestão de dados de internações hospitalares do SUS
no tópico Kafka 'datasus-internacoes'.

Uso:
    python producer.py [--bootstrap-server kafka:9092] [--topic datasus-internacoes] [--interval 1.0] [--batch-size 10]
=============================================================
"""

import json
import time
import random
import argparse
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── Dados de referência DATASUS ───
UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

# CIDs mais comuns em internações SUS
CIDS = {
    "A09": "Diarréia e gastroenterite de origem infecciosa presumível",
    "I10": "Hipertensão essencial (primária)",
    "J18": "Pneumonia por microrganismo não especificado",
    "K35": "Apendicite aguda",
    "O80": "Parto único espontâneo",
    "S72": "Fratura do fêmur",
    "I21": "Infarto agudo do miocárdio",
    "J44": "Outras doenças pulmonares obstrutivas crônicas",
    "E11": "Diabetes mellitus não insulino-dependente",
    "N39": "Outros transtornos do trato urinário",
    "C34": "Neoplasia maligna dos brônquios e pulmões",
    "I63": "Infarto cerebral",
    "K80": "Colelitíase",
    "J15": "Pneumonia bacteriana não classificada em outra parte",
    "A15": "Tuberculose respiratória, com confirmação bacteriológica e histológica",
}

FAIXAS_ETARIAS = [
    "0-4", "5-9", "10-14", "15-19", "20-29",
    "30-39", "40-49", "50-59", "60-69", "70-79", "80+"
]

SEXOS = ["M", "F"]

CARATERES_INTERNACAO = [
    "ELETIVA", "URGENCIA", "ACIDENTE_TRABALHO", "ACIDENTE_TRANSITO"
]


def gerar_internacao():
    """Gera um registro sintético de internação hospitalar SUS."""
    cid_codigo = random.choice(list(CIDS.keys()))
    uf = random.choice(UFS)
    faixa = random.choice(FAIXAS_ETARIAS)
    sexo = random.choice(SEXOS)

    # Datas de internação e alta
    data_internacao = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23)
    )
    dias_permanencia = random.randint(1, 30)
    data_alta = data_internacao + timedelta(days=dias_permanencia)

    # Valor da AIH (Autorização de Internação Hospitalar)
    valor_aih = round(random.uniform(200.0, 15000.0), 2)

    # Óbito (probabilidade de ~5%)
    obito = random.random() < 0.05

    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "uf": uf,
        "municipio_codigo": f"{random.randint(100000, 999999)}",
        "cnes": f"{random.randint(1000000, 9999999)}",  # Código do estabelecimento
        "cid_principal": cid_codigo,
        "cid_descricao": CIDS[cid_codigo],
        "sexo": sexo,
        "faixa_etaria": faixa,
        "idade": _idade_por_faixa(faixa),
        "carater_internacao": random.choice(CARATERES_INTERNACAO),
        "data_internacao": data_internacao.strftime("%Y-%m-%d %H:%M:%S"),
        "data_alta": data_alta.strftime("%Y-%m-%d %H:%M:%S"),
        "dias_permanencia": dias_permanencia,
        "valor_aih": valor_aih,
        "obito": obito,
        "uti": random.random() < 0.15,  # 15% chance UTI
        "procedimento_principal": f"0{random.randint(1,4)}0{random.randint(1,9)}0{random.randint(10,99)}0{random.randint(10,99)}",
    }


def _idade_por_faixa(faixa: str) -> int:
    """Retorna uma idade aleatória dentro da faixa etária."""
    ranges = {
        "0-4": (0, 4), "5-9": (5, 9), "10-14": (10, 14),
        "15-19": (15, 19), "20-29": (20, 29), "30-39": (30, 39),
        "40-49": (40, 49), "50-59": (50, 59), "60-69": (60, 69),
        "70-79": (70, 79), "80+": (80, 99)
    }
    r = ranges.get(faixa, (0, 99))
    return random.randint(r[0], r[1])


def criar_producer(bootstrap_server: str, max_retries: int = 30) -> KafkaProducer:
    """Cria o KafkaProducer com retry automático."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_server],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=16384,
                compression_type="gzip",
            )
            print(f"✅ Conectado ao Kafka em {bootstrap_server}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Tentativa {attempt}/{max_retries} - Kafka não disponível. Aguardando 5s...")
            time.sleep(5)

    raise RuntimeError(f"❌ Não foi possível conectar ao Kafka após {max_retries} tentativas")


def main():
    parser = argparse.ArgumentParser(description="Produtor Kafka - Dados DATASUS")
    parser.add_argument("--bootstrap-server", default="kafka:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="datasus-internacoes", help="Tópico Kafka")
    parser.add_argument("--interval", type=float, default=1.0, help="Intervalo entre lotes (segundos)")
    parser.add_argument("--batch-size", type=int, default=10, help="Mensagens por lote")
    parser.add_argument("--total", type=int, default=0, help="Total de mensagens (0 = infinito)")
    args = parser.parse_args()

    print("=" * 60)
    print("PRODUTOR KAFKA - DATASUS (Internações Hospitalares)")
    print("=" * 60)
    print(f"  Broker:     {args.bootstrap_server}")
    print(f"  Tópico:     {args.topic}")
    print(f"  Intervalo:  {args.interval}s")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Total:      {'infinito' if args.total == 0 else args.total}")
    print("=" * 60)

    producer = criar_producer(args.bootstrap_server)
    total_enviadas = 0

    try:
        while True:
            for _ in range(args.batch_size):
                internacao = gerar_internacao()

                # Usa UF como chave de partição (garante que mesma UF sempre vai pra mesma partição)
                producer.send(
                    args.topic,
                    key=internacao["uf"],
                    value=internacao,
                )
                total_enviadas += 1

            producer.flush()
            print(
                f"📤 [{datetime.now().strftime('%H:%M:%S')}] "
                f"Enviadas: {total_enviadas} mensagens | "
                f"Último lote: {args.batch_size} registros"
            )

            if args.total > 0 and total_enviadas >= args.total:
                print(f"\n✅ Total de {args.total} mensagens atingido. Encerrando.")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n⛔ Interrompido pelo usuário. Total enviadas: {total_enviadas}")
    finally:
        producer.close()
        print("Producer encerrado.")


if __name__ == "__main__":
    main()
