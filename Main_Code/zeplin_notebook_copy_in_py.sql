%flink.ssql

/*Option 'IF NOT EXISTS' can be used, to protect the existing Schema */
DROP TABLE IF EXISTS p50_stream1_records_table;

CREATE TABLE p50_stream1_records_table (
  `srcip` VARCHAR(50),
  `sport` bigint,
  `dstip` VARCHAR(50),
  `dsport` bigint,
  `proto` VARCHAR(50),
  `state` VARCHAR(50),
  `dur` VARCHAR(50),
  `sbytes` bigint,
  `dbytes` bigint,
  `sttl` bigint,
  `dttl` bigint,
  `sloss` bigint,
  `dloss` bigint,
  `service` VARCHAR(50),
  `sload` VARCHAR(50),
  `dload` double,
  `spkts` bigint,
  `dpkts` bigint,
  `swin` bigint,
  `dwin` bigint,
  `stcpb` bigint,
  `dtcpb` bigint,
  `smeansz` bigint,
  `dmeansz` bigint,
  `trans_depth` bigint,
  `res_bdy_len` bigint,
  `sjit` double,
  `djit` double,
  `stime` bigint,
  `ltime` bigint,
  `sintpkt` double,
  `dintpkt` double,
  `tcprtt` double,
  `synack` double,
  `ackdat` double,
  `is_sm_ips_ports` bigint,
  `ct_state_ttl` bigint,
  `ct_flw_http_mthd` bigint,
  `is_ftp_login` bigint,
  `ct_ftp_cmd` bigint,
  `ct_srv_src` bigint,
  `ct_srv_dst` bigint,
  `ct_dst_ltm` bigint,
  `ct_src_ ltm` bigint,
  `ct_src_dport_ltm` bigint,
  `ct_dst_sport_ltm` bigint,
  `ct_dst_src_ltm` bigint,
  `attack_cat` VARCHAR(50),
  `label` bigint,
  `Txn_Timestamp` TIMESTAMP(3)  
)
PARTITIONED BY (service)
WITH (
  'connector' = 'kinesis',
  'stream' = 'p50_raw_attack_records_stream',
  'aws.region' = 'us-east-2',
  'scan.stream.initpos' = 'LATEST',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);


/*Option 'IF NOT EXISTS' can be used, to protect the existing Schema */
DROP TABLE IF EXISTS p50_stream1_records_table_results;

CREATE TABLE p50_stream1_records_table_results (
  `srcip` VARCHAR(50),
  `sport` bigint,
  `dstip` VARCHAR(50),
  `dsport` bigint,
  `proto` VARCHAR(50),
  `state` VARCHAR(50),
  `dur` VARCHAR(50),
  `sbytes` bigint,
  `dbytes` bigint,
  `sttl` bigint,
  `dttl` bigint,
  `sloss` bigint,
  `dloss` bigint,
  `service` VARCHAR(50),
  `sload` VARCHAR(50),
  `dload` double,
  `spkts` bigint,
  `dpkts` bigint,
  `swin` bigint,
  `dwin` bigint,
  `stcpb` bigint,
  `dtcpb` bigint,
  `smeansz` bigint,
  `dmeansz` bigint,
  `trans_depth` bigint,
  `res_bdy_len` bigint,
  `sjit` double,
  `djit` double,
  `stime` bigint,
  `ltime` bigint,
  `sintpkt` double,
  `dintpkt` double,
  `tcprtt` double,
  `synack` double,
  `ackdat` double,
  `is_sm_ips_ports` bigint,
  `ct_state_ttl` bigint,
  `ct_flw_http_mthd` bigint,
  `is_ftp_login` bigint,
  `ct_ftp_cmd` bigint,
  `ct_srv_src` bigint,
  `ct_srv_dst` bigint,
  `ct_dst_ltm` bigint,
  `ct_src_ ltm` bigint,
  `ct_src_dport_ltm` bigint,
  `ct_dst_sport_ltm` bigint,
  `ct_dst_src_ltm` bigint,
  `attack_cat` VARCHAR(50),
  `label` bigint,
  `Txn_Timestamp` TIMESTAMP(3)
)
PARTITIONED BY (service)
WITH (
  'connector' = 'kinesis',
  'stream' = 'p50_dns_or_unknown_attack_records_stream',
  'aws.region' = 'us-east-2',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

insert into p50_stream1_records_table_results
select  *
from p50_stream1_records_table where service in ('-', 'dns');