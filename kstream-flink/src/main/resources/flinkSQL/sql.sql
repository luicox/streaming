-- Configuración inicial del entorno
SET 'execution.checkpointing.interval' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 'file:///tmp/flink/checkpoints';
SET 'execution.savepoint.ignore-unclaimed-state' = 'true';

-- Crear catálogo y base de datos
CREATE CATALOG memory_catalog WITH (
  'type' = 'generic_in_memory'
);
USE CATALOG memory_catalog;
CREATE DATABASE IF NOT EXISTS message_processor;
USE message_processor;

-- Tabla para mensajes principales (topico_origen)
CREATE TABLE source_messages (
  message_id STRING,
  content STRING,
  kafka_offset BIGINT METADATA FROM 'offset',
  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topico_origen',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink_sql_processor',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Tabla para mensajes técnicos (topico_zero_tecnico)
CREATE TABLE technical_messages (
  system_core_up INT,
  kafka_offset BIGINT METADATA FROM 'offset',
  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'topico_zero_tecnico',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink_sql_technical',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Tabla de destino (topico_destino)
CREATE TABLE destination_messages (
  message_id STRING,
  content STRING,
  processed_at TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'topico_destino',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Tabla de estado del sistema
CREATE TABLE system_status (
  status BOOLEAN,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (status) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'system_status_state',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- Vista para el último estado del sistema
CREATE VIEW latest_system_status AS
SELECT status, updated_at
FROM (
  SELECT status, updated_at,
         ROW_NUMBER() OVER (PARTITION BY status ORDER BY updated_at DESC) as row_num
  FROM system_status
) WHERE row_num = 1;

-- Procesamiento principal
INSERT INTO system_status
SELECT 
  CAST(system_core_up = 1 AS BOOLEAN) as status,
  kafka_timestamp as updated_at
FROM technical_messages;

-- Tabla temporal para mensajes en buffer
CREATE TABLE message_buffer (
  message_id STRING,
  content STRING,
  received_at TIMESTAMP(3),
  PRIMARY KEY (message_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'message_buffer_state',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- Almacenar mensajes en buffer
INSERT INTO message_buffer
SELECT 
  message_id,
  content,
  kafka_timestamp as received_at
FROM source_messages;

-- Enviar mensajes cuando el sistema está activo
INSERT INTO destination_messages
SELECT 
  b.message_id,
  b.content,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as processed_at
FROM message_buffer b
JOIN latest_system_status s ON s.status = TRUE
LEFT JOIN destination_messages d ON b.message_id = d.message_id
WHERE d.message_id IS NULL; -- Evitar duplicados

-- Limpiar buffer después de enviar (opcional)
-- DELETE FROM message_buffer
-- WHERE message_id IN (SELECT message_id FROM destination_messages);