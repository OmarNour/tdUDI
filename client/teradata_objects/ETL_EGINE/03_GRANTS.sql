GRANT EXECUTE 	PROCEDURE 	ON GDEV1_ETL TO GDEV1_ETL WITH GRANT OPTION;
GRANT EXECUTE 	FUNCTION 	ON GDEV1_ETL TO GDEV1_ETL WITH GRANT OPTION;

GRANT SELECT ON STG_ONLINE 			TO GDEV1V_STG_ONLINE WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_STG 			TO GDEV1V_STG WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_UTLFW 		TO GDEV1V_UTLFW WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_UTLFW 		TO GDEV1V_SRCI WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_UTLFW 		TO GDEV1V_SRCI WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_STG 			TO GDEV1V_SRCI WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_STG 			TO GDEV1V_SRCI WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_STG 			TO GDEV1V_INP WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_SRCI 		TO GDEV1V_INP WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_BASE 		TO GDEV1V_BASE WITH GRANT OPTION;

GRANT SELECT ON DBC 				TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_SRCI 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_SRCI 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_INP 			TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_UTLFW 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_BASE 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_BASE 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1V_STG_ONLINE 	TO GDEV1_ETL WITH GRANT OPTION;
GRANT SELECT ON GDEV1T_STG 			TO GDEV1_ETL WITH GRANT OPTION;

GRANT INSERT ON GDEV1T_UTLFW 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT INSERT ON GDEV1T_BASE 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT INSERT ON GDEV1T_STG 			TO GDEV1_ETL WITH GRANT OPTION;

GRANT UPDATE ON GDEV1T_BASE 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT UPDATE ON GDEV1T_STG 			TO GDEV1_ETL WITH GRANT OPTION;

GRANT DELETE ON GDEV1T_BASE 		TO GDEV1_ETL WITH GRANT OPTION;
GRANT DELETE ON GDEV1T_STG 			TO GDEV1_ETL WITH GRANT OPTION;

GRANT CREATE TABLE 	ON GDEV1T_SRCI 	TO GDEV1_ETL WITH GRANT OPTION;
GRANT DROP TABLE 	ON GDEV1T_SRCI 	TO GDEV1_ETL WITH GRANT OPTION;
GRANT CREATE TABLE 	ON GDEV1T_STG 	TO GDEV1_ETL WITH GRANT OPTION;
GRANT DROP TABLE 	ON GDEV1T_STG 	TO GDEV1_ETL WITH GRANT OPTION;

GRANT ALL ON GDEV1_ETL  			TO power_user  WITH GRANT OPTION; 
GRANT ALL ON STG_ONLINE  			TO power_user  WITH GRANT OPTION; 
GRANT ALL ON dev_stg_online			TO power_user  WITH GRANT OPTION; 
GRANT ALL ON GDEV1V_STG_ONLINE  	TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1T_STG  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1V_STG  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1V_INP  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1T_UTLFW  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1V_UTLFW  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1T_SRCI  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1V_SRCI  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1T_BASE  			TO power_user  WITH GRANT OPTION;
GRANT ALL ON GDEV1V_BASE  			TO power_user  WITH GRANT OPTION;



