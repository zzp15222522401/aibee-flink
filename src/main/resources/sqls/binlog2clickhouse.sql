CREATE TABLE mysql_binlog (
    `id` BIGINT,
    `topic` STRING,
    `partition_id` INT,
    `offset` BIGINT,
    `request_id` STRING,
    `site_id` STRING,
    `type` STRING,
    `area_id` STRING,
    `gate_id` STRING,
    `store_id` STRING,
    `floor` STRING,
    `event_type` INT,
    `camera_id` STRING,
    `send_time` BIGINT,
    `timestamp` BIGINT,
    `created_date` TIMESTAMP ,
    `updated_date` TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'data-db01.aibee.cn',
    'port' = '3306',
    'username' = 'openfaasdb_user',
    'password' = '5apj5R7s$y75S8z3',
    'database-name' = 'cjzhang_test',
    'table-name' = 'mall_person_times_event'
);
CREATE TABLE print_table (
    `id` BIGINT,
    `topic` STRING,
    `partition_id` INT,
    `offset` BIGINT,
    `request_id` STRING,
    `site_id` STRING,
    `type` STRING,
    `area_id` STRING,
    `gate_id` STRING,
    `store_id` STRING,
    `floor` STRING,
    `event_type` INT,
    `camera_id` STRING,
    `send_time` BIGINT,
    `timestamp` BIGINT,
    `created_date` TIMESTAMP,
    `updated_date` TIMESTAMP
) WITH (
      'connector' = 'print'
);
-- INSERT INTO print_table SELECT * FROM mysql_binlog;
CREATE TABLE tb_clickhouse_sink (
    `id` BIGINT,
    `topic` STRING,
    `partition_id` INT,
    `offset` BIGINT,
    `request_id` STRING,
    `site_id` STRING,
    `type` STRING,
    `area_id` STRING,
    `gate_id` STRING,
    `store_id` STRING,
    `floor` STRING,
    `event_type` INT,
    `camera_id` STRING,
    `send_time` BIGINT,
    `timestamp` BIGINT,
    `created_date` TIMESTAMP ,
    `updated_date` TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://172.16.244.65:8123',
    'table-name' = 'store_bi_test.mall_person_times_event',
    'username' = 'default',
    'password' = '123456',
    'format' = 'json'
);
INSERT INTO tb_clickhouse_sink SELECT * FROM mysql_binlog;
