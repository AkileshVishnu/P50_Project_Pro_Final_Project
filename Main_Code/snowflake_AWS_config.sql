------Using Storage Integration
--Create IAM Policy
/*
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::p50-bigdata-dns-or-unknown-attack-records/unsw-nb15/*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::p50-bigdata-dns-or-unknown-attack-records",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "Input/*"
                    ]
                }
            }
        }
    ]
}

----------------------------------------------------------------------------*/

--Create Storage Integration

CREATE or REPLACE STORAGE INTEGRATION P50_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::266931005008:role/P50_SNOWFLAKE_ACCESS_ROLE'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://p50-bigdata-dns-or-unknown-attack-records/unsw-nb15/dns_or_unknown_attack_records/');


desc INTEGRATION P50_S3_INTEGRATION;

--------------------------------------------------------------------------

--Create File Format

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true;

-----------------------------------------------------------------------

--Create Stage with Storage INTEGRATION

CREATE or REPLACE STAGE P50_DATA_STAGE
  URL='s3://p50-bigdata-dns-or-unknown-attack-records/unsw-nb15/dns_or_unknown_attack_records/dns_or_unknown_attack_records.csv'
  STORAGE_INTEGRATION = P50_S3_INTEGRATION
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

list @P50_DATA_STAGE;
--------------------------------------------------------------------

--Loading Data from Stage

--Load data into table from Stage
COPY INTO P50_FINAL_TABLE
  FROM @P50_DATA_STAGE
  PATTERN='.*.csv'; 


select * from P50_FINAL_DB.P50_FINAL_SCHEMA.P50_FINAL_TABLE;

--------------------------------------------------------------------------
/*Create Warehouse*/
/*Create Database*/
/*Create Schema*/
/*Table Creation*/

Use test_db;

create or replace TABLE P50_FINAL_DB.P50_FINAL_SCHEMA.P50_FINAL_TABLE (
	SRCIP VARCHAR(16777216),
	SPORT NUMBER(38,0),
	DSTIP VARCHAR(16777216),
	DSPORT NUMBER(38,0),
	PROTO VARCHAR(16777216),
	STATE VARCHAR(16777216),
	DUR FLOAT,
	SBYTES NUMBER(38,0),
	DBYTES NUMBER(38,0),
	STTL NUMBER(38,0),
	DTTL NUMBER(38,0),
	SLOSS NUMBER(38,0),
	DLOSS NUMBER(38,0),
	SERVICE VARCHAR(16777216),
	SLOAD FLOAT,
	DLOAD FLOAT,
	SPKTS NUMBER(38,0),
	DPKTS NUMBER(38,0),
	SWIN NUMBER(38,0),
	DWIN NUMBER(38,0),
	STCPB NUMBER(38,0),
	DTCPB NUMBER(38,0),
	SMEANSZ NUMBER(38,0),
	DMEANSZ NUMBER(38,0),
	TRANS_DEPTH NUMBER(38,0),
	RES_BDY_LEN NUMBER(38,0),
	SJIT FLOAT,
	DJIT FLOAT,
	STIME TIMESTAMP_NTZ(9),
	LTIME TIMESTAMP_NTZ(9),
	SINTPKT FLOAT,
	DINTPKT FLOAT,
	TCPRTT FLOAT,
	SYNACK FLOAT,
	ACKDAT FLOAT,
	IS_SM_IPS_PORTS NUMBER(38,0),
	CT_STATE_TTL NUMBER(38,0),
	CT_FLW_HTTP_MTHD NUMBER(38,0),
	IS_FTP_LOGIN NUMBER(38,0),
	CT_FTP_CMD NUMBER(38,0),
	CT_SRV_SRC NUMBER(38,0),
	CT_SRV_DST NUMBER(38,0),
	CT_DST_LTM NUMBER(38,0),
	CT_SRC_LTM NUMBER(38,0),
	CT_SRC_DPORT_LTM NUMBER(38,0),
	CT_DST_SPORT_LTM NUMBER(38,0),
	CT_DST_SRC_LTM NUMBER(38,0),
	ATTACK_CAT VARCHAR(16777216),
	LABEL NUMBER(38,0)
);



--Read data from table  
  
SELECT * FROM P50_FINAL_TABLE;  