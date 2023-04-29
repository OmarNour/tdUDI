INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('SRC', 'GDEV1V_STG_ONLINE', 'STG_ONLINE', 0) ;

INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('STG', 'GDEV1T_STG', 'GDEV1V_STG_ONLINE', 1) ;

INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('BKEY', 'GDEV1T_UTLFW', 'GDEV1V_INP', 2) ;

INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('BMAP', 'GDEV1T_UTLFW', 'GDEV1V_INP', 2) ;

INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('SRCI', 'GDEV1T_SRCI', 'GDEV1V_SRCI', 3) ;

INSERT INTO GDEV1_ETL.LAYERS  (layer_name, lyr_db, src_db, layer_Level)  
VALUES('CORE', 'GDEV1T_BASE', 'GDEV1V_INP', 4) ;