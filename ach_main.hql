USE ${dbName};
set mapreduce.job.queuename=${priorityQueue};
set mapred.child.java.opts=-Xmx4096m;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
set mapred.reduce.tasks=8;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


DROP TABLE IF EXISTS ACH_IMP4413_EDM_RUN_LOG;

CREATE EXTERNAL TABLE IF NOT EXISTS ACH_IMP4413_EDM_RUN_LOG(
RUN_NUMBER INT,
START_DATE STRING,
END_DATE STRING,
CATEGORY_NAME STRING,
ENTITY_NAME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${tablePath}/sqoop/IMP4413_EDM_RUN_LOG';


DROP TABLE IF EXISTS ACH_IMP4413_FEED_PARAMETERS;

CREATE EXTERNAL TABLE IF NOT EXISTS ACH_IMP4413_FEED_PARAMETERS(
PARAM_VALUE STRING,
PARAM_NAME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${tablePath}/sqoop/IMP4413_FEED_PARAMETERS';


DROP TABLE IF EXISTS ACH_MAIN;

CREATE EXTERNAL TABLE IF NOT EXISTS ACH_MAIN
(RECORDTYPE STRING,
BILLINGACCOUNTNUMBER STRING,
CHARGEID STRING,
CHARGEAMOUNT STRING,
CURRENCYCODE STRING,
PERCENTAGEALLOCATION STRING,
CHARGETYPE STRING,
CHARGESTARTDATE STRING,
CHARGEENDDATE STRING,
ASSETINSTANCEID STRING,
BILLINGINTERNALASSETID STRING,
PRODUCTCODE STRING,
BILLINGINTERNALPRODUCTCODE STRING,
TARIFFNAME STRING,
ASSETQUANTITY STRING,
QUANTITYMEASUREUNIT STRING,
CHARGERATE STRING,
CHARGERATEPERIODDURATION STRING,
CHARGERATEPERIODDURATIONUNITS STRING,
TAXIDENTIFIER STRING,
INVOICEID STRING,
INVOICEVERSION STRING,
INVOICEPRODUCTIONDATE STRING,
CUSTOMERSITEID STRING,
CUSTOMERORDERNUMBER STRING,
BTORDERNUMBER STRING,
REVENUECODE STRING,
ASSETCHARGEBILLDESCRIPTION STRING,
COSTCENTERCODE STRING,
SUMMARYCHARGEID STRING,
CHARGEPURPOSE STRING,
SERVICETOWERID STRING,
PRICINGRULEID STRING,
PRICELINEID STRING,
MAINASSETCHARGERECACTIONCODE STRING,
MAINASSETCHARGERECTIMESTAMP STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${tablePath}/sqoop/ACH_MAIN';





INSERT INTO TABLE BPP_SOG_ACH_ACHGM_V6_DNH_M2MH PARTITION(EXTRACT_DTM)
SELECT
RECORDTYPE ,
BILLINGACCOUNTNUMBER ,
CHARGEID ,
CHARGEAMOUNT ,
CURRENCYCODE ,
PERCENTAGEALLOCATION ,
CHARGETYPE ,
NVL(from_unixtime(UNIX_TIMESTAMP(CHARGESTARTDATE)),''),
NVL(from_unixtime(UNIX_TIMESTAMP(CHARGEENDDATE)),''),
ASSETINSTANCEID ,
BILLINGINTERNALASSETID ,
PRODUCTCODE ,
BILLINGINTERNALPRODUCTCODE ,
TARIFFNAME ,
ASSETQUANTITY ,
QUANTITYMEASUREUNIT ,
CHARGERATE ,
CHARGERATEPERIODDURATION ,
CHARGERATEPERIODDURATIONUNITS ,
TAXIDENTIFIER ,
INVOICEID ,
INVOICEVERSION ,
NVL(TO_DATE(INVOICEPRODUCTIONDATE),''),
CUSTOMERSITEID ,
CUSTOMERORDERNUMBER ,
BTORDERNUMBER ,
REVENUECODE ,
ASSETCHARGEBILLDESCRIPTION ,
COSTCENTERCODE ,
SUMMARYCHARGEID ,
CHARGEPURPOSE ,
SERVICETOWERID ,
PRICINGRULEID ,
PRICELINEID ,
MAINASSETCHARGERECACTIONCODE ,
MAINASSETCHARGERECTIMESTAMP ,
${RUN_NUMBER} as RUN_NUMBER,
${PARTITION_DATE} as EXTRACT_DTM
FROM ACH_MAIN;


SET hive.exec.compress.output=false;
SET mapreduce.output.fileoutputformat.compress=false;

DROP TABLE IF EXISTS BPP_SOG_ACH_ACHGM_V6_COUNT;
CREATE TABLE IF NOT EXISTS BPP_SOG_ACH_ACHGM_V6_COUNT
(
RECORD_COUNT STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${tablePath}/process/BPP_SOG_ACH_ACHGM_V6_COUNT';

INSERT INTO TABLE BPP_SOG_ACH_ACHGM_V6_COUNT
SELECT COUNT(*) FROM BPP_SOG_ACH_ACHGM_V6_DNH_M2MH WHERE RUN_NUMBER=${RUN_NUMBER} AND EXTRACT_DTM=${PARTITION_DATE};
