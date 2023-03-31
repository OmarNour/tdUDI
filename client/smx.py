import pandas as pd

from server.model import *


class SMX:
    run_id = str(generate_run_id())
    path = smx_path
    current_scripts_path = os.path.join(scripts_path, run_id)
    metadata_scripts = os.path.join(current_scripts_path, "metadata")
    log_error_path = current_scripts_path
    log_file_name = f"{run_id}.log"

    def __init__(self):
        create_folder(self.current_scripts_path)
        create_folder(self.metadata_scripts)

        logging.basicConfig(encoding='utf-8'
                            , level=logging.DEBUG
                            , format="%(asctime)s [%(levelname)s] %(message)s"
                            , handlers=[logging.FileHandler(os.path.join(self.log_error_path, self.log_file_name))
                                        # ,logging.StreamHandler()
                                        ]
                            )
        logging.info(f"Run ID {self.run_id}, started at {dt.datetime.now()}\n")

        self.xls = None
        self._source_systems = []
        # self.reserved_words = {}
        self.data = {}
        self.server = Server(server_name='TDVM')
        self.db_engine = DataBaseEngine(server_id=self.server.id, name=DB_NAME)
        Ip(server_id=self.server.id, ip='localhost')
        self.conn = Credential(db_engine_id=self.db_engine.id, user_name=USER, password=PASSWORD).get_connection()
        # print(self.conn)
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

        self.meta_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['META'].t_db))
        self.src_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRC'].t_db))
        self.stg_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['STG'].t_db))
        self.bmap_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BMAP'].t_db))
        self.bkey_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BKEY'].t_db))
        self.srci_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRCI'].t_db))
        self.core_t_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['CORE'].t_db))

        self.meta_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['META'].v_db))
        self.src_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRC'].v_db))
        self.stg_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['STG'].v_db))
        self.bmap_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BMAP'].v_db))
        self.bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['BKEY'].v_db))
        self.txf_bkey_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['TXF_BKEY'].v_db))
        self.srci_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['SRCI'].v_db))
        self.txf_core_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['TXF_CORE'].v_db))
        self.core_v_schema = Schema.get_instance(_key=(self.db_engine.id, LAYERS['CORE'].v_db))

    @log_error_decorator()
    @time_elapsed_decorator
    def parse_file(self):
        self.xls = pd.ExcelFile(self.path)
        for sheet in self.xls.sheet_names:
            self.parse_sheet(sheet)

        if 'supplements' in self.data.keys():
            reserved_words_df = self.data['supplements'].applymap(lambda x: x.lower())
            self.db_engine.reserved_words = reserved_words_df[reserved_words_df['reserved_words_source'] == self.db_engine.name]['reserved_words'].unique().tolist()

    @log_error_decorator()
    def parse_sheet(self, sheet):
        sheet_name = sheet.replace('  ', ' ').replace(' ', '_').lower()
        if sheet_name in SHEETS:
            df = self.xls.parse(sheet, encoding='utf-8').replace(np.nan, value='', regex=True)
            df = df.applymap(lambda x: x.replace('\ufeff', '').strip() if type(x) is str else int(x) if type(x) is float else x)
            df.drop_duplicates()
            df.columns = [c.replace('  ', ' ').replace(' ', '_').lower() for c in df]
            self.data[sheet_name] = df

    @property
    def sheet_names(self):
        return self.data.keys()

    @property
    def source_systems(self):
        return self._source_systems

    @log_error_decorator()
    @time_elapsed_decorator
    def populate_model(self, source_name):
        @log_error_decorator()
        def extract_all():
            @log_error_decorator()
            def extract_system(row):
                ds = DataSource(source_name=row.schema, source_level=1, scheduled=1)
                self._source_systems.append(ds.source_name)

            @log_error_decorator()
            def extract_data_types(row):
                data_type_lst = row.data_type.split(sep='(')
                DataType(db_id=self.db_engine.id, dt_name=data_type_lst[0], _override=1)

            @log_error_decorator()
            def extract_stg_tables(row):
                ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
                ds = DataSource.get_instance(_key=row.schema)
                assert ds, ds_error_msg

                src_t = Table(schema_id=self.src_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                stg_t = Table(schema_id=self.stg_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                srci_t = Table(schema_id=self.srci_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)

                src_lt = LayerTable(layer_id=self.src_layer.id, table_id=src_t.id)
                stg_lt = LayerTable(layer_id=self.stg_layer.id, table_id=stg_t.id)
                srci_lt = LayerTable(layer_id=self.srci_layer.id, table_id=srci_t.id)
                #############################################################
                src_v = Table(schema_id=self.src_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                src_lv = LayerTable(layer_id=self.src_layer.id, table_id=src_v.id)
                Pipeline(src_lyr_table_id=src_lt.id, tgt_lyr_table_id=stg_lt.id, lyr_view_id=src_lv.id)
                #############################################################
                stg_v = Table(schema_id=self.stg_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                stg_lv = LayerTable(layer_id=self.stg_layer.id, table_id=stg_v.id)
                Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=stg_lt.id, lyr_view_id=stg_lv.id)
                #############################################################
                srci_v = Table(schema_id=self.srci_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                srci_vl = LayerTable(layer_id=self.srci_layer.id, table_id=srci_v.id)
                Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=srci_lt.id, lyr_view_id=srci_vl.id)

            @log_error_decorator()
            def extract_core_tables(row):
                history_table = True if row.table_name in history_tables_lst else False
                table = Table(schema_id=self.core_t_schema.id, table_name=row.table_name, table_kind='T', history_table=history_table)
                LayerTable(layer_id=self.core_layer.id, table_id=table.id)

            @log_error_decorator()
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

            @log_error_decorator()
            def extract_src_view_columns(row):
                ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
                col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
                if row.natural_key == '':
                    ds = DataSource.get_instance(_key=row.schema)
                    assert ds, ds_error_msg

                    src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                    stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                    src_v = Table.get_instance(_key=(self.src_v_schema.id, row.table_name))
                    src_lv = LayerTable.get_instance(_key=(self.src_layer.id, src_v.id))

                    pipeline = Pipeline.get_instance(_key=src_lv.id)
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

            @log_error_decorator()
            def extract_stg_view_columns(row):
                ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
                col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
                if row.natural_key == '':
                    ds = DataSource.get_instance(_key=row.schema)
                    assert ds, ds_error_msg

                    stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                    stg_v = Table.get_instance(_key=(self.stg_v_schema.id, row.table_name))
                    stg_lv = LayerTable.get_instance(_key=(self.stg_layer.id, stg_v.id))

                    pipeline = Pipeline.get_instance(_key=stg_lv.id)
                    stg_col = Column.get_instance(_key=(stg_t.id, row.column_name))

                    assert stg_col, col_error_msg

                    ColumnMapping(pipeline_id=pipeline.id
                                  , col_seq=0
                                  , src_col_id=stg_col.id
                                  , tgt_col_id=stg_col.id
                                  , src_col_trx=None
                                  )

            @log_error_decorator()
            def extract_core_columns(row):
                core_table: Table
                core_table = Table.get_instance(_key=(self.core_t_schema.id, row.table_name))
                data_type_lst = row.data_type.split(sep='(')
                _data_type = data_type_lst[0]
                data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
                if data_type:
                    is_start_date = 1 if row.historization_key.upper() == 'S' else 0
                    is_end_date = 1 if row.historization_key.upper() == 'E' else 0
                    pk = 1 if (row.pk.upper() == 'Y' and not core_table.history_table) \
                              or (row.historization_key.upper() == 'Y' and core_table.history_table) \
                              or (is_start_date and core_table.history_table) else 0
                    mandatory = 1 if (row.mandatory.upper() == 'Y' or pk) else 0
                    precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None

                    if is_start_date:
                        sd_err_msg = f"Start date column '{row.column_name}', should be primary key as well!"
                        assert pk, sd_err_msg

                    Column(table_id=core_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                           , data_type_id=data_type.id, dt_precision=precision
                           , is_start_date=is_start_date, is_end_date=is_end_date)

            @log_error_decorator()
            def extract_bkey_tables(row):
                table = Table(schema_id=self.bkey_t_schema.id, table_name=row.physical_table, table_kind='T')
                LayerTable(layer_id=self.bkey_layer.id, table_id=table.id)

                Column(table_id=table.id, column_name='SOURCE_KEY', is_pk=1, mandatory=1
                       , data_type_id=vchar_data_type.id, dt_precision=100
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1
                       , data_type_id=int_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

                Column(table_id=table.id, column_name='EDW_KEY', is_pk=0, mandatory=1
                       , data_type_id=int_data_type.id, dt_precision=None
                       , is_start_date=0, is_end_date=0)

            @log_error_decorator()
            def extract_bkey_datasets(row):
                surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                set_table = Table.get_instance(_key=(self.core_t_schema.id, row.key_set_name))
                err_msg_set_table = f'Invalid set table {row.key_set_name}, \nphysical table is {row.physical_table}\n key_set_id: {row.key_set_id} '
                assert set_table, err_msg_set_table
                DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)

            @log_error_decorator()
            def extract_bkey_domains(row):
                data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
                err_msg_data_set = f'Invalid data set ID: {row.key_set_id}'
                assert data_set, err_msg_data_set
                Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)

            @log_error_decorator()
            def extract_bmap_tables(row):
                if row.physical_table != '':
                    table = Table(schema_id=self.bmap_t_schema.id, table_name=row.physical_table, table_kind='T')
                    LayerTable(layer_id=self.bmap_layer.id, table_id=table.id)

                    Column(table_id=table.id, column_name='SOURCE_CODE', is_pk=1, mandatory=1
                           , data_type_id=vchar_data_type.id, dt_precision=50
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

            @log_error_decorator()
            def extract_lookup_core_tables(row):
                if row.code_set_name != '':
                    table = Table(schema_id=self.core_t_schema.id, table_name=row.code_set_name, table_kind='T', _override=1)
                    LayerTable(layer_id=self.core_layer.id, table_id=table.id)

            @log_error_decorator()
            def extract_bmap_datasets(row):
                surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                set_table = Table.get_instance(_key=(self.core_t_schema.id, row.code_set_name))
                err_msg_set_table = f'Invalid set table {row.code_set_name}'
                assert set_table, err_msg_set_table
                DataSet(set_type_id=self.bmap_set_type.id, set_code=row.code_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)

            @log_error_decorator()
            def extract_bmap_domains(row):
                data_set = DataSet.get_instance(_key=(self.bmap_set_type.id, row.code_set_id))
                Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)

            @log_error_decorator()
            def extract_bmap_values(row):
                data_set_lst = [ds for ds in bmaps_data_sets if ds.set_table.table_name == row.code_set_name.lower()]
                if len(data_set_lst) > 0:
                    data_set = data_set_lst[0]
                    domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
                    if domain:
                        DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

            @log_error_decorator()
            def extract_bkey_txf_views(row):
                if row.natural_key != '' and row.key_domain_name != '':
                    ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
                    ds = DataSource.get_instance(_key=row.schema)
                    assert ds, ds_error_msg
                    domain: Domain
                    stg_col: Column
                    stg_t: Table
                    stg_lt: LayerTable
                    srci_col: Column

                    data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                    bkey_dataset_error_msg = f"key set '{row.key_set_name}', is not defined, please check the 'BKEY' sheet!"
                    assert data_set, bkey_dataset_error_msg

                    domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                    domain_error_msg = f"""{row.key_domain_name}, is not defined, please check the 'BKEY' sheet!"""
                    assert domain, domain_error_msg

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
                    bkey_v = Table(schema_id=self.txf_bkey_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)
                    bkey_vl = LayerTable(layer_id=self.txf_bkey_layer.id, table_id=bkey_v.id)

                    bkey_pipeline = Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=domain.data_set.surrogate_table.id, lyr_view_id=bkey_vl.id)
                    if row.bkey_filter:
                        Filter(pipeline_id=bkey_pipeline.id, filter_seq=1, complete_filter_expr=row.bkey_filter)

            @log_error_decorator()
            def extract_srci_view_columns(row):

                col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'

                stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))
                stg_t_col = None

                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                srci_t_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                if row.natural_key != '':
                    # nk_error_msg = f'Invalid natural key!,--> {row.natural_key}'
                    # stg_t_cols = [col for col in stg_table.columns if col.column_name in row.natural_key]
                    # assert stg_t_cols, nk_error_msg

                    if row.key_set_name != '':
                        data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                        bkey_dataset_error_msg = f"key set '{row.key_set_name}', is not defined, please check the 'BKEY' sheet!"
                        assert data_set, bkey_dataset_error_msg

                        domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                        domain_error_msg = f"""{row.key_domain_name}, is not defined, please check the 'BKEY' sheet!"""
                        assert domain, domain_error_msg

                        txf_view_name = BK_VIEW_NAME_TEMPLATE.format(src_lvl=stg_lt.layer.layer_level
                                                                     , src_table_name=stg_t.table_name
                                                                     , column_name=srci_t_col.column_name
                                                                     , tgt_lvl=self.txf_bkey_layer.layer_level
                                                                     , domain_id=srci_t_col.domain.domain_code
                                                                     )
                        # txf_view_name = f"BKEY_{row.table_name}_{row.column_name}_{srci_t_col.domain.domain_code}"
                        txf_v = Table.get_instance(_key=(self.txf_bkey_v_schema.id, txf_view_name))
                        txf_bkey_lt = LayerTable.get_instance(_key=(self.txf_bkey_layer.id, txf_v.id))
                        bkey_txf_pipeline = Pipeline.get_instance(_key=txf_bkey_lt.id)

                        # for col_seq, stg_t_col in enumerate(stg_t_cols):
                        source_key_col = Column.get_instance(_key=(domain.data_set.surrogate_table.id, 'source_key'))
                        ColumnMapping(pipeline_id=bkey_txf_pipeline.id
                                      , col_seq=0
                                      , src_col_id=None
                                      , tgt_col_id=source_key_col.id
                                      , src_col_trx=row.natural_key
                                      )
                        GroupBy(pipeline_id=bkey_txf_pipeline.id, col_id=source_key_col.id)

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
                srci_v = Table.get_instance(_key=(self.srci_v_schema.id, row.table_name))
                srci_vl = LayerTable.get_instance(_key=(self.srci_layer.id, srci_v.id))

                srci_v_pipeline = Pipeline.get_instance(_key=srci_vl.id)
                # for col_seq, stg_t_col in enumerate(stg_t_cols):
                ColumnMapping(pipeline_id=srci_v_pipeline.id
                              , col_seq=0
                              , src_col_id=stg_t_col.id if stg_t_col else None
                              , tgt_col_id=srci_t_col.id
                              , src_col_trx=row.natural_key if row.natural_key else None
                              )

            @log_error_decorator()
            def extract_core_txf_views(row):
                def parse_join(join_txt: str):
                    join_type: JoinType
                    with_srci_lt: LayerTable

                    join_type = None
                    _join_txt = ' ' + merge_multiple_spaces(join_txt).lower() + ' '
                    assert ' select ' or ' sel ' or '(sel ' or '(select ' not in _join_txt, 'Query cannot be found in join!'
                    _join_txt = _join_txt.replace(' inner ', ' ').replace(' outer ', ' ').strip()

                    new_input_join = 'join '

                    _split = _join_txt.split(' ', 1)
                    if len(_split) >= 2:
                        # print("===-=-=-=-=-=-=-=-==-=-=-====")
                        if _split[0].lower() == 'join':
                            # print('inner join found')
                            join_type = JoinType.get_instance(_key='ij')
                        elif _split[0].lower() == 'left':
                            # print('left join found')
                            join_type = JoinType.get_instance(_key='lj')
                        elif _split[0].lower() == 'right':
                            # print('right join found')
                            join_type = JoinType.get_instance(_key='rj')
                        elif _split[0].lower() == 'full':
                            # print('full outer join found')
                            join_type = JoinType.get_instance(_key='fj')

                        assert join_type, 'No join found!'

                        _split = _split[1].split(' on ', 1)
                        # print('on split: ', _split)
                        if len(_split) >= 2:
                            _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
                            _split_1 = _split[1]

                            table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
                            table_name = table__alias[0]
                            table_alias = table__alias[1] if len(table__alias) >= 2 else table_name
                            with_srci_t = Table.get_instance(_key=(self.srci_t_schema.id, table_name))
                            err_msg_invalid_srci_tbl = f'invalid join table, {table_name}'
                            assert with_srci_t, err_msg_invalid_srci_tbl
                            with_srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, with_srci_t.id))
                            # print(f"table name {table_name}, alias {table_alias}")
                            join_with = JoinWith(pipeline_id=core_pipeline.id
                                                 , master_lyr_table_id=srci_lt.id
                                                 , master_alias=core_pipeline.src_table_alias
                                                 , join_type_id=join_type.id
                                                 , with_lyr_table_id=with_srci_lt.id
                                                 , with_alias=table_alias)

                            _split = _split_1.split(' join ', 1)
                            join_on = _split[0]
                            if join_on.endswith(' left'):
                                join_on = join_on.removesuffix(' left')
                                new_input_join = 'left join '
                            elif join_on.endswith(' full'):
                                join_on = join_on.removesuffix(' full')
                                new_input_join = 'full join '
                            elif join_on.endswith(' right'):
                                join_on = join_on.removesuffix(' right')
                                new_input_join = 'right join '

                            # print(f"joined on {join_on}")
                            JoinOn(join_with_id=join_with.id, complete_join_on_expr=join_on)

                            # print("===-=-=-=-=-=-=-=-==-=-=-====")
                            if len(_split) >= 2:
                                parse_join(new_input_join + _split[1])

                @log_error_decorator()
                def column_mapping(_row):
                    # 'column_name', 'mapped_to_table', 'mapped_to_column', 'transformation_rule', 'transformation_type'
                    # transformation_type: COPY, SQL, CONST
                    transformation_type = _row.transformation_type.upper()
                    assert transformation_type in ('COPY', 'SQL', 'CONST'), "Transformation Type, should be one of the following COPY, SQL or CONST"
                    scd_type = 2 if _row.column_name in row.historization_columns else 1
                    tgt_col = Column.get_instance(_key=(core_t.id, _row.column_name))
                    err_msg_invalid_tgt_col = f'TXF - Invalid Target Column Name, {_row.column_name}'
                    assert tgt_col, err_msg_invalid_tgt_col

                    src_col = None
                    src_table_alias = None
                    src_table: Table
                    if transformation_type == 'COPY':
                        transformation_rule = None
                        if _row.mapped_to_column:
                            src_table_alias = _row.mapped_to_table
                            src_table = core_pipeline.get_table_by_alias(src_table_alias)
                            err_msg_invalid_src_tbl = f'Invalid Table, {src_table.table_name}'
                            src_t = Table.get_instance(_key=(self.srci_t_schema.id, src_table.table_name))
                            assert src_t, err_msg_invalid_src_tbl
                            src_col = Column.get_instance(_key=(src_t.id, _row.mapped_to_column))
                    elif transformation_type == 'CONST':
                        if str(_row.transformation_rule).upper() == '':
                            transformation_rule = None
                        else:
                            transformation_rule = single_quotes(_row.transformation_rule) if isinstance(_row.transformation_rule, str) else _row.transformation_rule
                    else:
                        # means SQL
                        src_table_alias = _row.mapped_to_table
                        transformation_rule = _row.transformation_rule

                    ColumnMapping(pipeline_id=core_pipeline.id
                                  , tgt_col_id=tgt_col.id
                                  , src_col_id=src_col.id if src_col else None
                                  , src_table_alias=src_table_alias
                                  , col_seq=0
                                  , src_col_trx=transformation_rule if transformation_rule else None
                                  , constant_value=True if transformation_type == 'CONST' else False
                                  , scd_type=scd_type
                                  )

                ###########################################################################################################
                core_t: Table
                ds_error_msg = f"""{row.source}, is not defined, please check the 'System' sheet!"""
                ds = DataSource.get_instance(_key=row.source)
                assert ds, ds_error_msg

                # if row.mapped_to == '':
                main_tables_name = row.main_source.split(',')
                main_table_name = main_tables_name[0].strip()
                main_table_alias = row.main_source_alias if row.main_source_alias else main_table_name
                # else:
                #     main_tables_name = row.mapped_to.split(',')
                #     main_table_name__alias = main_tables_name[0].split(' ')
                #     main_table_name__alias = [item for item in main_table_name__alias if item != '']
                #     main_table_name = main_table_name__alias[0]
                #     main_table_alias = main_table_name__alias[1] if len(main_table_name__alias) >= 2 else row.main_source_alias

                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, main_table_name))
                err_msg_invalid_main_tbl = f'invalid main table, {main_table_name}'
                assert srci_t, err_msg_invalid_main_tbl
                srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))

                core_t = Table.get_instance(_key=(self.core_t_schema.id, row.target_table_name))
                core_lt = LayerTable.get_instance(_key=(self.core_layer.id, core_t.id))

                txf_view_name = CORE_VIEW_NAME_TEMPLATE.format(mapping_name=row.mapping_name)
                core_txf_v = Table(schema_id=self.txf_core_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)
                # [Column(table_id=core_txf_v.id, column_name=col.column_name) for col in core_t.columns]
                core_txf_vl = LayerTable(layer_id=self.txf_core_layer.id, table_id=core_txf_v.id)

                core_pipeline = Pipeline(src_lyr_table_id=srci_lt.id
                                         , tgt_lyr_table_id=core_lt.id
                                         , src_table_alias=main_table_alias
                                         , lyr_view_id=core_txf_vl.id)

                parse_join(row.join)

                if row.filter_criterion:
                    Filter(pipeline_id=core_pipeline.id, filter_seq=1, complete_filter_expr=row.filter_criterion)

                if 'column_mapping' in self.data.keys():
                    column_mapping_df = filter_dataframe(self.data['column_mapping'], 'mapping_name', row.mapping_name)
                    column_mapping_df[['column_name'
                        , 'mapped_to_table'
                        , 'mapped_to_column'
                        , 'transformation_rule'
                        , 'transformation_type']].drop_duplicates().apply(column_mapping, axis=1)

            ####################################################  Begin DFs  ####################################################
            assert 'system' in self.data.keys(), "'System' sheet!, does not exists!"

            _core_tables = []
            _core_tables_bkey = []
            _core_tables_bmap = []
            _source_names = None
            if source_name:
                _source_names = source_name if isinstance(source_name, list) else [source_name]
                _source_names.extend(UNIFIED_SOURCE_SYSTEMS)

            system_df = filter_dataframe(self.data['system'], 'schema', _source_names)
            assert not system_df.empty, "No Source systems found in the 'System' sheet!"
            assert 'stg_tables' in self.data.keys(), "'Stg Tables' sheet, does not exists!"

            stg_tables_df = filter_dataframe(self.data['stg_tables'], 'schema', _source_names)

            table_mapping_df = pd.DataFrame()
            if 'table_mapping' in self.data.keys():
                table_mapping_df = filter_dataframe(filter_dataframe(self.data['table_mapping'], 'source', _source_names), 'layer', 'CORE')
                _core_tables = [i for i in set(table_mapping_df['target_table_name'].values.tolist()) if i]

            bkey_df = pd.DataFrame()
            if 'bkey' in self.data.keys():
                bkey_df = filter_dataframe(self.data['bkey'], 'key_domain_name', [i for i in set(stg_tables_df['key_domain_name'].values.tolist()) if i])
                _core_tables_bkey = [i for i in set(bkey_df['key_set_name'].values.tolist()) if i]

            bmap_df = pd.DataFrame()
            bmap_values_df = pd.DataFrame()
            if 'bmap' in self.data.keys():
                bmap_df = filter_dataframe(self.data['bmap'], 'code_domain_name', [i for i in set(stg_tables_df['code_domain_name'].values.tolist()) if i])
                _core_tables_bmap = [i for i in set(bmap_df['code_set_name'].values.tolist()) if i]
                if 'bmap_values' in self.data.keys():
                    bmap_values_df = filter_dataframe(self.data['bmap_values'], 'code_domain_id', [i for i in set(bmap_df['code_domain_id'].values.tolist()) if i])

            all_core_tables = list(set(_core_tables + _core_tables_bkey + _core_tables_bmap))
            core_tables_df = pd.DataFrame()
            history_tables_lst = []
            if 'core_tables' in self.data.keys():
                core_tables_df = filter_dataframe(self.data['core_tables'], 'table_name', all_core_tables)
                history_core_tables_df = filter_dataframe(core_tables_df, 'historization_key', 'Y')
                if not history_core_tables_df.empty:
                    history_tables_lst = history_core_tables_df['table_name'].drop_duplicates().tolist()

            ####################################################  End DFs  ####################################################
            ####################################################  Begin   ####################################################
            system_df.drop_duplicates().apply(extract_system, axis=1)
            stg_tables_df[['data_type']].drop_duplicates().apply(extract_data_types, axis=1)
            if not core_tables_df.empty:
                core_tables_df[['data_type']].drop_duplicates().apply(extract_data_types, axis=1)

            int_data_type = DataType.get_instance(_key=(self.db_engine.id, 'INTEGER'))
            vchar_data_type = DataType.get_instance(_key=(self.db_engine.id, 'VARCHAR'))

            if not core_tables_df.empty:
                core_tables_df[['table_name']].drop_duplicates().apply(extract_core_tables, axis=1)

            if not bkey_df.empty:
                bkey_df[['physical_table']].drop_duplicates().apply(extract_bkey_tables, axis=1)

            if not bmap_df.empty:
                bmap_df[['physical_table']].drop_duplicates().apply(extract_bmap_tables, axis=1)

            stg_tables_df[['schema', 'table_name']].drop_duplicates().apply(extract_stg_tables, axis=1)
            ##########################  Start bkey & bmaps   #####################
            if not bkey_df.empty:
                bkey_df[['key_set_name', 'key_set_id', 'physical_table']].drop_duplicates().apply(extract_bkey_datasets, axis=1)
                bkey_df[['key_set_id', 'key_domain_id', 'key_domain_name']].drop_duplicates().apply(extract_bkey_domains, axis=1)

            if not bmap_df.empty:
                bmap_df[['code_set_name', 'code_set_id', 'physical_table']].drop_duplicates().apply(extract_bmap_datasets, axis=1)

            bmaps_data_sets = DataSetType.get_instance(_key=DS_BMAP).data_sets

            if not bmap_df.empty:
                bmap_df[['code_set_id', 'code_domain_id', 'code_domain_name']].drop_duplicates().apply(extract_bmap_domains, axis=1)

            if not bmap_values_df.empty:
                bmap_values_df[['code_set_name', 'code_domain_id', 'edw_code', 'source_code', 'description']].drop_duplicates().apply(extract_bmap_values, axis=1)
            ##########################  End bkey & bmaps     #####################

            stg_tables_df[['table_name', 'column_name', 'data_type', 'mandatory', 'natural_key', 'pk',
                           'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_stg_srci_table_columns, axis=1)

            stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name', 'column_transformation_rule']].drop_duplicates().apply(extract_src_view_columns, axis=1)
            stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name']].drop_duplicates().apply(extract_stg_view_columns, axis=1)

            ##########################  Start bkey TXF view  #####################
            stg_tables_df[['schema', 'table_name', 'natural_key', 'column_name'
                , 'key_set_name', 'key_domain_name', 'bkey_filter']].drop_duplicates().apply(extract_bkey_txf_views, axis=1)
            # extract_bkey_txf_columns
            ##########################  End bkey TXF view    #####################
            stg_tables_df[['schema', 'table_name', 'column_name', 'natural_key'
                , 'key_set_name', 'key_domain_name']].drop_duplicates().apply(extract_srci_view_columns, axis=1)
            ##########################      Start Core TXF view     #####################
            if not core_tables_df.empty:
                core_tables_df[['table_name', 'column_name', 'data_type', 'pk', 'mandatory', 'historization_key']].drop_duplicates().apply(extract_core_columns, axis=1)

            if not table_mapping_df.empty:
                table_mapping_df[
                    [
                        'target_table_name', 'source'
                        , 'main_source', 'main_source_alias'
                        , 'filter_criterion', 'mapping_name', 'join'
                        , 'historization_algorithm', 'historization_columns'
                    ]
                ].drop_duplicates().apply(extract_core_txf_views, axis=1)
            ##########################      End Core TXF view       #####################
            ####################################################  End   ####################################################
            logging.info(f'DataSetType count: {DataSetType.count_instances()}')
            logging.info(f'Layer count: {Layer.count_instances()}')
            logging.info(f'Schema count: {Schema.count_instances()}')
            logging.info(f'DataType count: {DataType.count_instances()}')
            logging.info(f'DataSource count: {DataSource.count_instances()}')
            logging.info(f'Table count: {Table.count_instances()}')
            logging.info(f'LayerTable count: {LayerTable.count_instances()}')
            logging.info(f'DataSet count: {DataSet.count_instances()}')
            logging.info(f'Domain count: {Domain.count_instances()}')
            logging.info(f'DomainValue count: {DomainValue.count_instances()}')
            logging.info(f'Column count: {Column.count_instances()}')
            logging.info(f'Pipeline count: {Pipeline.count_instances()}')
            logging.info(f'ColumnMapping count: {ColumnMapping.count_instances()}')
            logging.info(f'JoinWith count: {JoinWith.count_instances()}')
            logging.info(f'JoinOn count: {JoinOn.count_instances()}')
            logging.info(f'Filter count: {Filter.count_instances()}')
            logging.info(f'GroupBy count: {GroupBy.count_instances()}')

            # MyID.serialize_all()

        extract_all()


def layer_table_scripts(row):
    row.layer_table: LayerTable
    ddl = row.layer_table.table.ddl
    if ddl:
        kind_folder = 'tables' if row.layer_table.table.table_kind == 'T' else 'views'
        tables_file = WriteFile(row.out_path, kind_folder, "sql", 'a')
        tables_file.write(ddl)
        tables_file.write('\n')
        tables_file.close()

    dml = row.layer_table.dml
    if dml:
        # data_path = os.path.dirname(row.out_path)
        data_file = WriteFile(row.out_path, 'data', "sql", 'a')
        data_file.write(dml)
        data_file.close()


def generate_schemas_ddl(smx):
    db_file = WriteFile(smx.current_scripts_path, "schemas", "sql")
    for schema in Schema.get_all_instances():
        ddl = schema.ddl
        if ddl:
            db_file.write(ddl)
            db_file.write("\n")
    db_file.close()


@time_elapsed_decorator
def generate_scripts(smx: SMX):
    source_dict: dict = {}
    core_model_path = os.path.join(smx.current_scripts_path, CORE_MODEL_FOLDER_NAME)
    src_systems_path = os.path.join(smx.current_scripts_path, SRC_SYSTEMS_FOLDER_NAME)

    # create_folder(core_model_path)

    for src in smx.source_systems:
        source_path = os.path.join(smx.current_scripts_path, src_systems_path, src)
        source_dict[src.lower()] = source_path
        # create_folder(source_path)
        del source_path

    def layer_table_out_path(row):
        layer_table: LayerTable
        layer_table = row.layer_table

        if layer_table.table.data_source:
            main_folder = source_dict[layer_table.table.data_source.source_name.lower()]
        else:
            main_folder = core_model_path

        layer_folder_name = f"Layer_{layer_table.layer.layer_level}_{layer_table.layer.layer_name}"

        path = os.path.join(smx.current_scripts_path, main_folder, layer_folder_name)
        return path

    layer_tables_df = pd.DataFrame(LayerTable.get_all_instances(), columns=['layer_table'])
    layer_tables_df['out_path'] = layer_tables_df.apply(layer_table_out_path, axis=1)
    layer_tables_df[['out_path']].drop_duplicates().apply(lambda row: create_folder(row.out_path), axis=1)

    # print('start generating scripts!')
    # layer_tables_df.apply(layer_table_scripts, axis=1)
    layer_tables_df.swifter.apply(layer_table_scripts, axis=1)


@time_elapsed_decorator
def generate_metadata_scripts(smx: SMX):
    """
    todo:
        scripts to populate the below tables:
            source_name_lkp
            source_table_lkp
            etl_process
            history
            gcfr_process
            gcfr_transfer_key_col

    :param smx:
    :return:
    """
    # INSERT_INTO_SOURCE_NAME_LKP smx.metadata_scripts
    src_name_lkp = WriteFile(smx.metadata_scripts, 'source_name_lkp', "sql")
    src_tables_lkp = WriteFile(smx.metadata_scripts, 'source_tables_lkp', "sql")
    gcfr_transform_keycol = WriteFile(smx.metadata_scripts, 'gcfr_transform_keycol', "sql")
    etl_process = WriteFile(smx.metadata_scripts, 'etl_process', "sql")

    source: DataSource
    table: Table
    pk_col: Column
    pipeline: Pipeline

    for source in DataSource.get_all_instances():
        src_name_lkp.write(INSERT_INTO_SOURCE_NAME_LKP.format(meta_db=smx.meta_v_schema.schema_name,
                                                              SOURCE_NAME=source.source_name,
                                                              rejection_table_name='NULL',
                                                              business_rules_table_name='NULL',
                                                              LOADING_TYPE='ONLINE',
                                                              SOURCE_DB=smx.src_v_schema.schema_name,
                                                              DATA_SRC_CD='NULL'
                                                              ))
        for table in source.tables:
            if table.schema.id == smx.src_v_schema.id:
                src_tables_lkp.write(INSERT_INTO_SOURCE_TABLES_LKP.format(meta_db=smx.meta_v_schema.schema_name,
                                                                          SOURCE_NAME=source.source_name,
                                                                          TABLE_NAME=table.table_name,
                                                                          TRANSACTION_DATA=1 if table.transactional_data else 0
                                                                          ))
    for table in Table.get_all_instances():
        for pk_col in table.key_col:
            gcfr_transform_keycol.write(INSERT_INTO_GCFR_TRANSFORM_KEYCOL.format(meta_db=smx.meta_v_schema.schema_name,
                                                                                 OUT_DB_NAME=table.schema.schema_name,
                                                                                 OUT_OBJECT_NAME=table.table_name,
                                                                                 KEY_COLUMN=pk_col.column_name
                                                                                 ))

    for pipeline in Pipeline.get_all_instances():
        if pipeline.tgt_lyr_table:
            process_type = None
            apply_type = None
            if pipeline.lyr_view.layer.id == smx.txf_bkey_layer.id:
                process_type = 'BKEY'
                apply_type = 'INSERT'
            elif pipeline.lyr_view.layer.id == smx.txf_core_layer.id:
                process_type = 'TXF'
                apply_type = 'UPSERT'  # to be revisited
            if process_type:
                etl_process.write(INSERT_INTO_ETL_PROCESS.format(meta_db=smx.meta_v_schema.schema_name,
                                                                 SOURCE_NAME=pipeline.src_lyr_table.table.data_source.source_name,
                                                                 PROCESS_TYPE=process_type,
                                                                 PROCESS_NAME=pipeline.lyr_view.table.table_name,
                                                                 BASE_TABLE=pipeline.tgt_lyr_table.table.table_name,
                                                                 APPLY_TYPE=apply_type,
                                                                 INPUT_VIEW_DB=pipeline.lyr_view.table.schema.schema_name,
                                                                 TARGET_TABLE_DB=pipeline.tgt_lyr_table.table.schema.schema_name,
                                                                 TARGET_VIEW_DB='NULL',
                                                                 SRCI_TABLE_DB=pipeline.src_lyr_table.table.schema.schema_name,
                                                                 SRCI_TABLE_NAME=pipeline.src_lyr_table.table.table_name,
                                                                 KEY_SET_ID='NULL',
                                                                 DOMAIN_ID='NULL',
                                                                 CODE_SET_ID='NULL',
                                                                 ))

    src_name_lkp.close()
    src_tables_lkp.close()
    gcfr_transform_keycol.close()
    etl_process.close()


@time_elapsed_decorator
def deploy(scripts_path):
    pass
