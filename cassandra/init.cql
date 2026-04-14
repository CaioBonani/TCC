-- =============================================================
-- Cassandra Schema Initialization
-- Lambda Architecture - TCC DATASUS
-- Keyspace: lambda_arch
-- =============================================================

-- Criação do Keyspace principal
CREATE KEYSPACE IF NOT EXISTS lambda_arch
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE lambda_arch;

-- =============================================================
-- BATCH VIEW
-- Visões consolidadas geradas pelo Batch Layer (Spark Batch)
-- Reprocessamento completo dos dados históricos do HDFS
-- =============================================================
CREATE TABLE IF NOT EXISTS batch_view (
    -- Chaves de partição: UF + ano/mês para distribuição eficiente
    uf TEXT,
    ano INT,
    mes INT,
    -- Métricas agregadas
    total_internacoes BIGINT,
    total_obitos BIGINT,
    total_valor DOUBLE,
    tempo_medio_permanencia DOUBLE,
    -- Detalhamento por faixa etária
    internacoes_por_faixa_etaria MAP<TEXT, BIGINT>,
    -- Detalhamento por CID (Classificação Internacional de Doenças)
    internacoes_por_cid MAP<TEXT, BIGINT>,
    -- Metadados de processamento
    processado_em TIMESTAMP,
    versao_batch INT,
    PRIMARY KEY ((uf, ano), mes)
) WITH CLUSTERING ORDER BY (mes DESC)
  AND comment = 'Visão consolidada do Batch Layer - reprocessamento completo HDFS';

-- =============================================================
-- SPEED VIEW
-- Visões near-realtime geradas pelo Speed Layer (Spark Streaming)
-- Agregações em janelas móveis de 1 segundo
-- =============================================================
CREATE TABLE IF NOT EXISTS speed_view (
    -- Chaves de partição: UF + janela temporal
    uf TEXT,
    janela_inicio TIMESTAMP,
    janela_fim TIMESTAMP,
    -- Métricas incrementais
    total_internacoes BIGINT,
    total_obitos BIGINT,
    total_valor DOUBLE,
    -- Detalhamento
    internacoes_por_cid MAP<TEXT, BIGINT>,
    -- Metadados
    processado_em TIMESTAMP,
    PRIMARY KEY ((uf), janela_inicio, janela_fim)
) WITH CLUSTERING ORDER BY (janela_inicio DESC, janela_fim DESC)
  AND comment = 'Visão near-realtime do Speed Layer - streaming Kafka'
  AND default_time_to_live = 604800;  -- TTL 7 dias (dados substituídos pelo Batch)

-- =============================================================
-- MERGED VIEW
-- Visão unificada da Serving Layer
-- Combina resultados do Batch e Speed Layers
-- A seleção entre visão rápida ou histórica depende do timestamp
-- =============================================================
CREATE TABLE IF NOT EXISTS merged_view (
    -- Chaves de partição
    uf TEXT,
    ano INT,
    mes INT,
    -- Fonte dos dados
    fonte TEXT,  -- 'batch' ou 'speed'
    -- Métricas
    total_internacoes BIGINT,
    total_obitos BIGINT,
    total_valor DOUBLE,
    tempo_medio_permanencia DOUBLE,
    -- Metadados
    atualizado_em TIMESTAMP,
    PRIMARY KEY ((uf, ano), mes, fonte)
) WITH CLUSTERING ORDER BY (mes DESC, fonte ASC)
  AND comment = 'Visão unificada Serving Layer - merge Batch + Speed';

-- =============================================================
-- TABELA DE CONTROLE - Rastreamento de processamento
-- =============================================================
CREATE TABLE IF NOT EXISTS controle_processamento (
    tipo_processamento TEXT,   -- 'batch' ou 'speed'
    ultima_execucao TIMESTAMP,
    offset_kafka BIGINT,
    status TEXT,               -- 'sucesso', 'erro', 'em_andamento'
    detalhes TEXT,
    PRIMARY KEY (tipo_processamento)
) WITH comment = 'Controle de execuções do Batch e Speed Layers';

-- Inserir registros iniciais de controle
INSERT INTO controle_processamento (tipo_processamento, ultima_execucao, offset_kafka, status, detalhes)
VALUES ('batch', toTimestamp(now()), 0, 'aguardando', 'Nenhum processamento batch realizado');

INSERT INTO controle_processamento (tipo_processamento, ultima_execucao, offset_kafka, status, detalhes)
VALUES ('speed', toTimestamp(now()), 0, 'aguardando', 'Nenhum processamento streaming realizado');
