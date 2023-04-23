REPLACE PROCEDURE GDEV1_ETL.get_delta(
  IN i_src_db VARCHAR(30),
  IN i_src_tbl VARCHAR(30),
  IN i_tgt_db VARCHAR(30),
  IN i_tgt_tbl VARCHAR(30),
  OUT o_inserted_rows_count INTEGER,
  OUT o_updated_rows_count INTEGER,
  OUT o_deleted_rows_count INTEGER,
  OUT o_delta_table_name INTEGER,
  OUT o_return_code INTEGER,
  OUT o_return_message VARCHAR(100)
)
BEGIN
  DECLARE v_sql VARCHAR(10000);
  DECLARE v_rowcount INTEGER;

  SET v_sql =  'CREATE VOLATILE TABLE vt_delta AS (
			    SELECT * FROM (
			      SELECT ''I'' AS row_type, s.* FROM (
			        SELECT * FROM i_src_db.i_src_tbl
			        MINUS
			        SELECT * FROM i_tgt_db.i_tgt_tbl
			      ) s
			      UNION ALL
			      SELECT ''U'' AS row_type, s.* FROM (
			        SELECT * FROM i_src_db.i_src_tbl
			        INTERSECT
			        SELECT * FROM i_tgt_db.i_tgt_tbl
			      ) s
			    ) d
			  ) WITH DATA
			  ON COMMIT PRESERVE ROWS;';
  
  CALL DBC.SysExecSQL(v_sql, v_rowcount);

  SET v_sql = 'UPDATE i_tgt_db.i_tgt_tbl t ' ||
             'SET (col1, col2, col3) = ' ||
             '(SELECT col1, col2, col3 FROM vt_delta d WHERE d.row_type = ''U'' AND t.primary_key = d.primary_key);';

  CALL DBC.SysExecSQL(v_sql, v_rowcount);

  SET o_updated_rows_count = v_rowcount;

  SET v_sql = 'INSERT INTO i_tgt_db.i_tgt_tbl ' ||
             'SELECT * FROM vt_delta WHERE row_type = ''I'';';

  CALL DBC.SysExecSQL(v_sql, v_rowcount);

  SET o_inserted_rows_count = v_rowcount;

  SET v_sql = 'DELETE FROM i_tgt_db.i_tgt_tbl ' ||
             'WHERE primary_key IN (SELECT primary_key FROM vt_delta) ' ||
             'AND primary_key NOT IN (SELECT primary_key FROM i_src_db.i_src_tbl);';

  CALL DBC.SysExecSQL(v_sql, v_rowcount);

  SET o_deleted_rows_count = v_rowcount;

  DROP TABLE vt_delta;

  SET o_return_code = 0;
  SET o_return_message = 'Success';
END;
