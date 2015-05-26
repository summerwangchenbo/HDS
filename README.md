# HDS
过渡层

##############################################################################################
##逻辑主题:ODS SNP
##
##脚本名称:dw_ods.dw_ods_snp_tb_paction.ins.sql
##
##执行逻辑:全量加载
##
##执行频率:按小时
##
##
##版本管理:20150525 脚本初始化      Summer (summer.wang@hunteron.com)
##############################################################################################
SET @BATCH_NAME =  "dw_ods.dw_ods_snp_tb_paction.ins.sql";
INSERT INTO DW_METADATA_BATCH_STATUS (BATCH_NAME) VALUES (@BATCH_NAME);
##############################################################################################

/*

DROP TABLE IF EXISTS DW_ODS_SNP_TB_PACTION_T_BK;
RENAME TABLE DW_ODS_SNP_TB_PACTION_T TO DW_ODS_SNP_TB_PACTION_T_BK;

CREATE TABLE `DW_ODS_SNP_TB_PACTION_T` (
  `ID` BIGINT(20) NOT NULL,
  `ZIP_BEG_DATE` DATETIME DEFAULT NULL,
  `ZIP_END_DATE` DATETIME DEFAULT NULL,  
  `ACTIVE` int(11) DEFAULT NULL COMMENT '合同是否活动 0 合同未生效, 1 合同生效, 2 合同失效, 3 猎头同意协议',
  `ACTIVE_DES` VARCHAR(30) DEFAULT NULL COMMENT '合同是否活动 0 合同未生效, 1 合同生效, 2 合同失效, 3 猎头同意协议'
) ENGINE=INNODB DEFAULT CHARSET=UTF8
;

ALTER TABLE `DW_ODS_SNP_TB_PACTION_T` ADD INDEX DW_ODS_SNP_TB_PACTION_T_IDX01 ( `ID` ) ;
ALTER TABLE `DW_ODS_SNP_TB_PACTION_T` ADD INDEX DW_ODS_SNP_TB_PACTION_T_IDX02 ( `ZIP_END_DATE` ) ;


CREATE OR REPLACE VIEW DW_ODS_SNP_TB_PACTION AS
SELECT
 ID
,ZIP_BEG_DATE
,ZIP_END_DATE
,ACTIVE
,ACTIVE_DES
FROM DW_ODS_SNP_TB_PACTION_T
;

*/

##############################################################################################
#staging
##############################################################################################

DROP TABLE IF EXISTS TMP_DW_ODS_SNP_TB_PACTION_T;
CREATE TEMPORARY TABLE TMP_DW_ODS_SNP_TB_PACTION_T AS(
SELECT
A.ID,
CURRENT_TIMESTAMP()              AS ZIP_BEG_DATE,
TIMESTAMP('3000-12-31 00:00:00') AS ZIP_END_DATE,
A.ACTIVE,
CASE WHEN A.ACTIVE = 0 THEN "合同未生效"
     WHEN A.ACTIVE = 1 THEN "合同生效"
     WHEN A.ACTIVE = 2 THEN "合同失效"
     WHEN A.ACTIVE = 3 THEN "猎头同意协议"
END AS ACTIVE_DES
FROM DW_HDS_TB_PACTION A
)
;
ALTER TABLE `TMP_DW_ODS_SNP_TB_PACTION_T` ADD INDEX TMP_DW_ODS_SNP_TB_PACTION_T_IDX01 ( `ID` ) ;

##############################################################################################
#snp loading and update
##############################################################################################

INSERT INTO DW_ODS_SNP_TB_PACTION_T
(
 ID
,ZIP_BEG_DATE
,ZIP_END_DATE
,ACTIVE 
,ACTIVE_DES       
)
SELECT
 STG.ID
,STG.ZIP_BEG_DATE
,STG.ZIP_END_DATE
,STG.ACTIVE   
,STG.ACTIVE_DES     
FROM TMP_DW_ODS_SNP_TB_PACTION_T STG
LEFT OUTER JOIN DW_ODS_SNP_TB_PACTION_T TGT ON STG.ID = TGT.ID AND STG.ZIP_BEG_DATE = TGT.ZIP_BEG_DATE
WHERE TGT.ID IS NULL AND TGT.ZIP_BEG_DATE IS NULL
;



UPDATE DW_ODS_SNP_TB_PACTION_T TGT INNER JOIN TMP_DW_ODS_SNP_TB_PACTION_T STG
SET
   TGT.ZIP_END_DATE = STG.ZIP_BEG_DATE
WHERE TGT.ID = STG.ID AND TGT.ZIP_END_DATE = TIMESTAMP('3000-12-31 00:00:00')
AND TGT.ZIP_BEG_DATE <> STG.ZIP_BEG_DATE
