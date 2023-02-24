from server.model import *


class SMX:

    @time_elapsed_decorator
    def __init__(self, smx_path, scripts_path):
        self.run_id = str(generate_run_id())
        print(f"Run ID {self.run_id}, started at {dt.datetime.now()}\n")
        self.path = smx_path
        self.scripts_path = scripts_path

        self.current_scripts_path = os.path.join(self.scripts_path, self.run_id)
        self.log_error_path = self.current_scripts_path
        create_folder(self.current_scripts_path)
        self.xls = None
        # self.reserved_words = {}
        self.data = {}
        self.db_engine = DataBaseEngine(name=DB_NAME)
        [LayerType(type_name=lt) for lt in LAYER_TYPES]
        for layer_key, layer_value in LAYERS.items():
            layer_type = LayerType.get_instance(_key=layer_value.type)
            Layer(type_id=layer_type.id, layer_name=layer_key, abbrev=layer_key, layer_level=layer_value.level)
            if layer_value.t_db:
                Schema(db_id=self.db_engine.id, schema_name=layer_value.t_db, _override=1)
            if layer_value.v_db:
                Schema(db_id=self.db_engine.id, schema_name=layer_value.v_db, _override=1)

        DataSetType(name=DS_BKEY)
        DataSetType(name=DS_BMAP)

        self.bkey_set_type = DataSetType.get_instance(_key=DS_BKEY)
        self.bmap_set_type = DataSetType.get_instance(_key=DS_BMAP)

        self.src_layer = Layer.get_instance(_key='SRC')
        self.stg_layer = Layer.get_instance(_key='STG')
        self.txf_bkey_layer = Layer.get_instance(_key='TXF_BKEY')
        self.bmap_layer = Layer.get_instance(_key='BMAP')
        self.bkey_layer = Layer.get_instance(_key='BKEY')
        self.srci_layer = Layer.get_instance(_key='SRCI')
        self.txf_core_layer = Layer.get_instance(_key='TXF_CORE')
        self.core_layer = Layer.get_instance(_key='CORE')

        self.src_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRC'].t_db))
        self.stg_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['STG'].t_db))
        self.bmap_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BMAP'].t_db))
        self.bkey_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BKEY'].t_db))
        self.srci_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRCI'].t_db))
        self.core_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['CORE'].t_db))

        self.src_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRC'].v_db))
        self.stg_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['STG'].v_db))
        self.bmap_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BMAP'].v_db))
        self.bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BKEY'].v_db))
        self.txf_bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['TXF_BKEY'].v_db))
        self.srci_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRCI'].v_db))
        self.txf_core_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['TXF_CORE'].v_db))
        self.core_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['CORE'].v_db))

    @time_elapsed_decorator
    def parse_file(self):
        self.xls = pd.ExcelFile(self.path)
        for sheet in self.xls.sheet_names:
            self.parse_sheet(sheet)

        reserved_words_df = self.data['supplements'].applymap(lambda x: x.lower())
        self.db_engine.reserved_words = reserved_words_df[reserved_words_df['reserved_words_source'] == self.db_engine.name]['reserved_words'].unique().tolist()

    def parse_sheet(self, sheet):
        sheet_name = sheet.replace('  ', ' ').replace(' ', '_').lower()
        if sheet_name in SHEETS:
            df = self.xls.parse(sheet).replace(np.nan, value='', regex=True)
            df = df.applymap(lambda x: x.replace('\ufeff', '').strip() if type(x) is str else int(x) if type(x) is float else x)
            df.drop_duplicates()
            df.columns = [c.replace('  ', ' ').replace(' ', '_').lower() for c in df]
            self.data[sheet_name] = df

    @property
    def sheet_names(self):
        return self.data.keys()

    @time_elapsed_decorator
    @log_error_decorator(None)
    def extract_all(self):

        @log_error_decorator(self.log_error_path)
        def extract_system(row):
            DataSource(source_name=row.schema, source_level=1, scheduled=1)

        @log_error_decorator(self.log_error_path)
        def extract_data_types(row):
            data_type_lst = row.data_type.split(sep='(')
            DataType(db_id=self.db_engine.id, dt_name=data_type_lst[0], _override=1)

        @log_error_decorator(self.log_error_path)
        def extract_stg_tables(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds, ds_error_msg

            src_table = Table(schema_id=self.src_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
            stg_table = Table(schema_id=self.stg_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
            srci_table = Table(schema_id=self.srci_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)

            LayerTable(layer_id=self.src_layer.id, table_id=src_table.id)
            LayerTable(layer_id=self.stg_layer.id, table_id=stg_table.id)
            LayerTable(layer_id=self.srci_layer.id, table_id=srci_table.id)

        @log_error_decorator(self.log_error_path)
        def extract_core_tables(row):
            table = Table(schema_id=self.core_t_schema.id, table_name=row.table_name, table_kind='T')
            LayerTable(layer_id=self.core_layer.id, table_id=table.id)

        @log_error_decorator(self.log_error_path)
        def extract_stg_srci_table_columns(row):
            data_type_error_msg = f"Data type {row.data_type} for {row.table_name}.{row.column_name} column is invalid, please check 'Stg tables' sheet! "
            bkey_dataset_error_msg = f"key set '{row.key_set_name}', is not defined"
            bmap_dataset_error_msg = f"code set '{row.code_set_name}', is not defined"

            src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
            stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))

            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))

            assert data_type, data_type_error_msg

            pk = 1 if row.pk.upper() == 'Y' else 0
            mandatory = 1 if row.pk.upper() == 'Y' else 0
            precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None

            if row.natural_key == '':
                domain_id = None
                if src_table:
                    Column(table_id=src_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

                if stg_table:
                    Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)
            else:
                domain = None
                domain_error_msg = f"Domain not found for {row.table_name}.{row.column_name} column, please check 'Stg tables' sheet!"
                if row.key_set_name != '' and row.key_domain_name != '':

                    data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                    assert data_set, bkey_dataset_error_msg

                    domain = Domain.get_by_name(data_set.id, row.key_domain_name)
                    domain_error_msg = f"Bkey Domain not found for {row.table_name}.{row.column_name} column, " \
                                       f"\n key set name:'{row.key_set_name}'" \
                                       f"\n key domain name: '{row.key_domain_name}'" \
                                       f"\n please check 'Stg tables' sheet!"

                elif row.code_set_name != '' and row.code_domain_name != '':

                    data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                    assert data_set, bmap_dataset_error_msg

                    domain = Domain.get_by_name(data_set.id, row.code_domain_name)
                    domain_error_msg = f"Bmap Domain not found for {row.table_name}.{row.column_name} column, " \
                                       f"\n code set name:'{row.code_set_name}'" \
                                       f"\n code domain name: '{row.code_domain_name}'" \
                                       f"\n please check 'Stg tables' sheet!"

                assert domain, domain_error_msg
                domain_id = domain.id

            if srci_table:
                Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                       , data_type_id=data_type.id, dt_precision=precision, domain_id=domain_id)

        @log_error_decorator(self.log_error_path)
        def extract_src_views(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds, ds_error_msg

            src_v = Table(schema_id=self.src_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
            LayerTable(layer_id=self.src_layer.id, table_id=src_v.id)

            src_t = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
            stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

            src_lt = LayerTable.get_instance(_key=(self.src_layer.id, src_t.id))
            stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

            Pipeline(src_lyr_table_id=src_lt.id, tgt_lyr_table_id=stg_lt.id, table_id=src_v.id)

        @log_error_decorator(self.log_error_path)
        def extract_stg_views(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds, ds_error_msg

            stg_v = Table(schema_id=self.stg_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
            LayerTable(layer_id=self.stg_layer.id, table_id=stg_v.id)

            stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

            Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=stg_lt.id, table_id=stg_v.id)

        @log_error_decorator(self.log_error_path)
        def extract_src_view_columns(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
            if row.natural_key == '':
                ds = DataSource.get_instance(_key=row.schema)
                assert ds, ds_error_msg

                src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                src_lyr_table = LayerTable.get_instance(_key=(self.src_layer.id, src_table.id))
                stg_lyr_table = LayerTable.get_instance(_key=(self.stg_layer.id, stg_table.id))

                pipeline = Pipeline.get_instance(_key=(src_lyr_table.id, stg_lyr_table.id))
                src_col = Column.get_instance(_key=(src_table.id, row.column_name))
                stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                assert src_col, col_error_msg
                assert stg_col, col_error_msg

                ColumnMapping(pipeline_id=pipeline.id
                              , col_seq=0
                              , src_col_id=src_col.id
                              , tgt_col_id=stg_col.id
                              , src_col_trx=row.column_transformation_rule
                              )

        @log_error_decorator(self.log_error_path)
        def extract_stg_view_columns(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
            if row.natural_key == '':
                ds = DataSource.get_instance(_key=row.schema)
                assert ds, ds_error_msg

                stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                stg_lyr_table = LayerTable.get_instance(_key=(self.stg_layer.id, stg_table.id))

                pipeline = Pipeline.get_instance(_key=(stg_lyr_table.id, stg_lyr_table.id))
                stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                assert stg_col, col_error_msg

                ColumnMapping(pipeline_id=pipeline.id
                              , col_seq=0
                              , src_col_id=stg_col.id
                              , tgt_col_id=stg_col.id
                              , src_col_trx=None
                              )

        @log_error_decorator(self.log_error_path)
        def extract_core_columns(row):
            core_table = Table.get_instance(_key=(self.core_t_schema.id, row.table_name))
            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
            if data_type:
                pk = 1 if row.pk.upper() == 'Y' else 0
                mandatory = 1 if row.mandatory.upper() == 'Y' else 0
                is_start_date = 1 if row.historization_key.upper() == 'S' else 0
                is_end_date = 1 if row.historization_key.upper() == 'E' else 0
                precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                Column(table_id=core_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                       , data_type_id=data_type.id, dt_precision=precision
                       , is_start_date=is_start_date, is_end_date=is_end_date)

        @log_error_decorator(self.log_error_path)
        def extract_bkey_tables(row):
            table = Table(schema_id=self.bkey_t_schema.id, table_name=row.physical_table, table_kind='T')
            LayerTable(layer_id=self.bkey_layer.id, table_id=table.id)

            Column(table_id=table.id, column_name='SOURCE_KEY', is_pk=1, mandatory=1
                   , data_type_id=vchar_data_type.id, dt_precision=None
                   , is_start_date=0, is_end_date=0)

            Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1
                   , data_type_id=int_data_type.id, dt_precision=None
                   , is_start_date=0, is_end_date=0)

            Column(table_id=table.id, column_name='EDW_KEY', is_pk=0, mandatory=1
                   , data_type_id=int_data_type.id, dt_precision=None
                   , is_start_date=0, is_end_date=0)

        @log_error_decorator(self.log_error_path)
        def extract_bkey_datasets(row):
            table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
            DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_name=row.key_set_name, table_id=table.id)

        @log_error_decorator(self.log_error_path)
        def extract_bkey_domains(row):
            data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
            Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)

        @log_error_decorator(self.log_error_path)
        def extract_bmap_tables(row):
            if row.physical_table != '':
                table = Table(schema_id=self.bmap_t_schema.id, table_name=row.physical_table, table_kind='T')
                LayerTable(layer_id=self.bmap_layer.id, table_id=table.id)

                Column(table_id=table.id, column_name='SOURCE_CODE', is_pk=1, mandatory=1
                       , data_type_id=vchar_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1
                       , data_type_id=int_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='CODE_SET_ID', is_pk=1, mandatory=1
                       , data_type_id=int_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='EDW_CODE', is_pk=0, mandatory=1
                       , data_type_id=int_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='DESCRIPTION', is_pk=0, mandatory=1
                       , data_type_id=vchar_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

        @log_error_decorator(self.log_error_path)
        def extract_lookup_core_tables(row):
            if row.code_set_name != '':
                table = Table(schema_id=self.core_t_schema.id, table_name=row.code_set_name, table_kind='T')
                LayerTable(layer_id=self.core_layer.id, table_id=table.id)

        @log_error_decorator(self.log_error_path)
        def extract_bmap_datasets(row):
            table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
            DataSet(set_type_id=self.bmap_set_type.id, set_code=row.code_set_id, set_name=row.code_set_name, table_id=table.id)

        @log_error_decorator(self.log_error_path)
        def extract_bmap_domains(row):
            data_set = DataSet.get_instance(_key=(self.bmap_set_type.id, row.code_set_id))
            Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)

        @log_error_decorator(self.log_error_path)
        def extract_bmap_values(row):
            data_set_lst = [ds for ds in bmaps_data_sets if ds.set_name == row.code_set_name]
            if len(data_set_lst) > 0:
                data_set = data_set_lst[0]
                domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
                if domain:
                    DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

        @log_error_decorator(self.log_error_path)
        def extract_bkey_txf_views(row):
            if row.natural_key != '' and row.key_set_name != '':
                ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
                ds = DataSource.get_instance(_key=row.schema)
                assert ds, ds_error_msg
                utlfw_t: Table
                stg_col: Column
                stg_t: Table
                stg_lt: LayerTable
                srci_col: Column

                stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                srci_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                txf_view_name = BK_VIEW_NAME_TEMPLATE.format(src_lvl=stg_lt.layer.layer_level
                                                             , src_table_name=stg_t.table_name
                                                             , column_name=srci_col.column_name
                                                             , tgt_lvl=self.txf_bkey_layer.layer_level
                                                             , domain_id=srci_col.domain.domain_code
                                                             )
                # txf_view_name = f"BKEY_{row.table_name}_{row.column_name}_{srci_col.domain.domain_code}"
                txf_v = Table(schema_id=self.txf_bkey_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)
                txf_bkey_lt = LayerTable(layer_id=self.txf_bkey_layer.id, table_id=txf_v.id)

                bkey_pipeline = Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=txf_bkey_lt.id)
                if row.bkey_filter:
                    Filter(pipeline_id=bkey_pipeline.id, filter_seq=1, complete_filter_expr=row.bkey_filter)

        @log_error_decorator(self.log_error_path)
        def extract_srci_views(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds, ds_error_msg

            srci_v = Table(schema_id=self.srci_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
            LayerTable(layer_id=self.srci_layer.id, table_id=srci_v.id)

            stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

            srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
            srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))

            Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=srci_lt.id, table_id=srci_v.id)

        @log_error_decorator(self.log_error_path)
        def extract_srci_view_columns(row):

            col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'

            stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))

            srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
            srci_lyr_table = LayerTable.get_instance(_key=(self.srci_layer.id, srci_table.id))

            stg_t_col = None
            srci_t_col = Column.get_instance(_key=(srci_table.id, row.column_name))

            if row.natural_key != '':
                # nk_error_msg = f'Invalid natural key!,--> {row.natural_key}'
                # stg_t_cols = [col for col in stg_table.columns if col.column_name in row.natural_key]
                # assert stg_t_cols, nk_error_msg

                if row.key_set_name != '':
                    txf_view_name = BK_VIEW_NAME_TEMPLATE.format(src_lvl=stg_lt.layer.layer_level
                                                                 , src_table_name=stg_t.table_name
                                                                 , column_name=srci_t_col.column_name
                                                                 , tgt_lvl=self.txf_bkey_layer.layer_level
                                                                 , domain_id=srci_t_col.domain.domain_code
                                                                 )
                    # txf_view_name = f"BKEY_{row.table_name}_{row.column_name}_{srci_t_col.domain.domain_code}"
                    txf_v = Table.get_instance(_key=(self.txf_bkey_v_schema.id, txf_view_name))
                    txf_bkey_lt = LayerTable.get_instance(_key=(self.txf_bkey_layer.id, txf_v.id))
                    bkey_txf_v_col = Column(table_id=txf_v.id, column_name='source_key')
                    bkey_txf_pipeline = Pipeline.get_instance(_key=(stg_lt.id, txf_bkey_lt.id))
                    # for col_seq, stg_t_col in enumerate(stg_t_cols):
                    ColumnMapping(pipeline_id=bkey_txf_pipeline.id
                                  , col_seq=0
                                  , src_col_id=None
                                  , tgt_col_id=bkey_txf_v_col.id
                                  , src_col_trx=row.natural_key
                                  )
                    GroupBy(pipeline_id=bkey_txf_pipeline.id, col_id=bkey_txf_v_col.id)

            else:
                stg_t_col = Column.get_instance(_key=(stg_t.id, row.column_name))
                assert stg_t_col, col_error_msg
            #
            #
            #     # weird_txt = self.clean_trx(row.natural_key, extra=[col.column_name for col in stg_table.columns])
            #     # trx_error_msg = f'invalid transformation for {row.schema}, {row.table_name}.{row.column_name} ' \
            #     #                 f'\n-> {row.natural_key} ' \
            #     #                 f'\n-xx->{weird_txt}<-xx-'
            #     #
            #     # assert weird_txt == '' or weird_txt == 'null', trx_error_msg

            assert srci_t_col, col_error_msg

            srci_v_pipeline = Pipeline.get_instance(_key=(stg_lt.id, srci_lyr_table.id))
            # for col_seq, stg_t_col in enumerate(stg_t_cols):
            ColumnMapping(pipeline_id=srci_v_pipeline.id
                          , col_seq=0
                          , src_col_id=stg_t_col.id if stg_t_col else None
                          , tgt_col_id=srci_t_col.id
                          , src_col_trx=row.natural_key if row.natural_key else None
                          )

        self.data['system'].drop_duplicates().apply(extract_system, axis=1)
        self.data['stg_tables'][['data_type']].drop_duplicates().apply(extract_data_types, axis=1)
        self.data['core_tables'][['data_type']].drop_duplicates().apply(extract_data_types, axis=1)

        int_data_type = DataType.get_instance(_key=(self.db_engine.id, 'INTEGER'))
        vchar_data_type = DataType.get_instance(_key=(self.db_engine.id, 'VARCHAR'))
        ##########################  Start bkey & bmaps   #####################
        self.data['bkey'][['physical_table']].drop_duplicates().apply(extract_bkey_tables, axis=1)
        self.data['bkey'][['key_set_name', 'key_set_id', 'physical_table']].drop_duplicates().apply(extract_bkey_datasets, axis=1)
        self.data['bkey'][['key_set_id', 'key_domain_id', 'key_domain_name']].drop_duplicates().apply(extract_bkey_domains, axis=1)

        self.data['bmap'][['physical_table']].drop_duplicates().apply(extract_bmap_tables, axis=1)
        self.data['bmap'][['code_set_name']].drop_duplicates().apply(extract_lookup_core_tables, axis=1)

        self.data['bmap'][['code_set_name', 'code_set_id', 'physical_table']].drop_duplicates().apply(extract_bmap_datasets, axis=1)
        bmaps_data_sets = DataSetType.get_instance(_key='BMAP').data_sets
        self.data['bmap'][['code_set_id', 'code_domain_id', 'code_domain_name']].drop_duplicates().apply(extract_bmap_domains, axis=1)
        self.data['bmap_values'][['code_set_name', 'code_domain_id', 'edw_code', 'source_code', 'description']].drop_duplicates().apply(extract_bmap_values, axis=1)
        ##########################  End bkey & bmaps     #####################

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_stg_tables, axis=1)
        self.data['stg_tables'][['table_name', 'column_name', 'data_type', 'mandatory', 'natural_key', 'pk',
                                 'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_stg_srci_table_columns, axis=1)

        self.data['core_tables'][['table_name']].drop_duplicates().apply(extract_core_tables, axis=1)
        self.data['core_tables'][['table_name', 'column_name', 'data_type', 'pk', 'mandatory', 'historization_key']].drop_duplicates().apply(extract_core_columns, axis=1)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_src_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'natural_key', 'column_name', 'column_transformation_rule']].drop_duplicates().apply(extract_src_view_columns, axis=1)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_stg_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'natural_key', 'column_name']].drop_duplicates().apply(extract_stg_view_columns, axis=1)

        ##########################  Start bkey TXF view  #####################
        self.data['stg_tables'][['schema', 'table_name', 'natural_key', 'column_name', 'key_set_name', 'bkey_filter']].drop_duplicates().apply(extract_bkey_txf_views, axis=1)
        # extract_bkey_txf_columns
        ##########################  End bkey TXF view    #####################

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_srci_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'column_name', 'natural_key', 'key_set_name']].drop_duplicates().apply(extract_srci_view_columns, axis=1)

        ##########################      Start Core TXF view     #####################
        # extract_core_txf_views
        # extract_core_txf_view_columns
        ##########################      End Core TXF view       #####################

        print('DataSetType count:', len(DataSetType.get_all_instances()))
        print('Layer count:', len(Layer.get_all_instances()))
        print('Schema count:', len(Schema.get_all_instances()))
        print('DataType count:', len(DataType.get_all_instances()))
        print('DataSource count:', len(DataSource.get_all_instances()))
        print('Table count:', len(Table.get_all_instances()))
        print('LayerTable count:', len(LayerTable.get_all_instances()))
        print('DataSet count:', len(DataSet.get_all_instances()))
        print('Domain count:', len(Domain.get_all_instances()))
        print('DomainValue count:', len(DomainValue.get_all_instances()))
        print('Column count:', len(Column.get_all_instances()))
        print('Pipeline count:', len(Pipeline.get_all_instances()))
        print('ColumnMapping count:', len(ColumnMapping.get_all_instances()))
        print('Filter count:', len(Filter.get_all_instances()))
        print('GroupBy count:', len(GroupBy.get_all_instances()))

    @time_elapsed_decorator
    def generate_scripts(self, source_name: str = None):
        _source_name = None
        if source_name is not None:
            _source_name = source_name.lower()

        def layer_scripts(layer: Layer):
            @log_error_decorator(None)
            def layer_table_scripts(layer_table: LayerTable):
                try:
                    lyr_src_name = None
                    if _source_name is not None:
                        lyr_src_name = layer_table.table.data_source.source_name
                except:
                    lyr_src_name = None

                if lyr_src_name == _source_name or lyr_src_name is None:
                    ddl = layer_table.table.ddl
                    dml = layer_table.dml
                    if ddl:
                        if layer_table.table.table_kind == 'T':
                            tables_ddl.append(ddl)
                        elif layer_table.table.table_kind == 'V':
                            views_ddl.append(ddl)
                    if dml:
                        tables_dml.append(dml)

            layer_folder_name = f"Layer_{layer.layer_level}_{layer.layer_name}"
            layer_path = os.path.join(self.current_scripts_path, layer_folder_name)
            create_folder(layer_path)

            tables_ddl = []
            views_ddl = []
            tables_dml = []
            threads(layer_table_scripts, layer.layer_tables)
            # for layer_table in layer.layer_tables:

            ################ START WRITE TO FILES ################
            if tables_ddl:
                tables_ddl = list(filter(None, tables_ddl))
                tables_ddl = "\n".join(tables_ddl)
                tables_file = WriteFile(layer_path, 'tables', "sql")
                tables_file.write(tables_ddl)
                tables_file.close()

            if views_ddl:
                views_ddl = list(filter(None, views_ddl))
                views_ddl = "\n".join(views_ddl)
                views_file = WriteFile(layer_path, 'views', "sql")
                views_file.write(views_ddl)
                views_file.close()

            if tables_dml:
                tables_dml = list(filter(None, tables_dml))
                tables_dml = "\n".join(tables_dml)
                dml_file = WriteFile(layer_path, 'data', "sql")
                dml_file.write(tables_dml)
                dml_file.close()
            ################ END WRITE TO FILES ################

        threads(layer_scripts, Layer.get_all_instances())
        open_folder(self.current_scripts_path)
