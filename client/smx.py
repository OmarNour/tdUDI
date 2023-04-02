import logging

import pandas as pd

from server.model import *


class SMX:

    def __init__(self):
        self.run_id = str(generate_run_id())
        print(f"Run started with ID: {self.run_id}")
        self.path = smx_path
        self.current_scripts_path = os.path.join(scripts_path, self.run_id)
        self.metadata_scripts = os.path.join(self.current_scripts_path, "metadata")
        self.log_error_path = self.current_scripts_path
        self.log_file_name = f"{self.run_id}.log"
        create_folder(self.current_scripts_path)
        create_folder(self.metadata_scripts)
        separator = "**********************************************************************************"
        logging.basicConfig(encoding='utf-8'
                            , level=logging.DEBUG
                            , format=f"[%(levelname)s] %(message)s\n{separator}\n"
                            , handlers=[logging.FileHandler(os.path.join(self.log_error_path, self.log_file_name))
                                        # ,logging.StreamHandler()
                                        ]
                            )
        logging.info(f"Run ID {self.run_id}, started at {dt.datetime.now()}\n")

        self.xls = None
        self._source_systems = []
        # self.reserved_words = {}
        self.data = {}
        self.init_model()

    @log_error_decorator()
    def init_model(self):
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
                Schema(db_id=self.db_engine.id, schema_name=layer_value.t_db, _raise_if_exist=0)
            if layer_value.v_db:
                Schema(db_id=self.db_engine.id, schema_name=layer_value.v_db, _raise_if_exist=0)

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
                DataType(db_id=self.db_engine.id, dt_name=data_type_lst[0], _raise_if_exist=0)

            @log_error_decorator()
            def extract_stg_tables(row):
                ds = DataSource.get_instance(_key=row.schema)
                if ds:
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
                else:
                    logging.error(f""" Invalid Source name, {row.schema}, processing row:\n{row}""")

            @log_error_decorator()
            def extract_core_tables(row):
                history_table = True if row.table_name in history_tables_lst else False
                table = Table(schema_id=self.core_t_schema.id, table_name=row.table_name, table_kind='T', history_table=history_table)
                LayerTable(layer_id=self.core_layer.id, table_id=table.id)

            @log_error_decorator()
            def extract_stg_srci_table_columns(row):
                src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))

                data_type_lst = row.data_type.split(sep='(')
                _data_type = data_type_lst[0]
                data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
                if not data_type:
                    logging.error(f"Invalid data type, processing row:\n{row}")
                else:
                    pk = 1 if row.pk.upper() == 'Y' else 0
                    mandatory = 1 if row.pk.upper() == 'Y' else 0
                    precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                    domain_id = None

                    if row.natural_key == '':
                        if src_table:
                            Column(table_id=src_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

                        if stg_table:
                            Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)
                    else:
                        domain = None
                        domain_error_msg = f"Domain not found for {row.table_name}.{row.column_name} column, please check 'Stg tables' sheet!"
                        if row.key_set_name and row.key_domain_name:

                            data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                            if not data_set:
                                logging.error(f"invalid key set name '{row.key_set_name}', processing row:\n{row}")
                            else:
                                domain = Domain.get_by_name(data_set.id, row.key_domain_name)
                                domain_error_msg = f"Bkey Domain not found for {row.table_name}.{row.column_name} column, " \
                                                   f"\n key set name:'{row.key_set_name}'" \
                                                   f"\n key domain name: '{row.key_domain_name}'" \
                                                   f"\n please check 'Stg tables' sheet!"

                        elif row.code_set_name and row.code_domain_name:

                            data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                            if not data_set:
                                logging.error(f"invalid code set name '{row.code_set_name}', processing row:\n{row}")
                            else:
                                domain = Domain.get_by_name(data_set.id, row.code_domain_name)
                                domain_error_msg = f"Bmap Domain not found for {row.table_name}.{row.column_name} column, " \
                                                   f"\n code set name:'{row.code_set_name}'" \
                                                   f"\n code domain name: '{row.code_domain_name}'" \
                                                   f"\n please check 'Stg tables' sheet!"

                        if domain:
                            domain_id = domain.id
                        else:
                            logging.error(f"{domain_error_msg}, processing row:\n{row}")

                    if srci_table:
                        Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                               , data_type_id=data_type.id, dt_precision=precision, domain_id=domain_id)

            @log_error_decorator()
            def extract_src_view_columns(row):
                if row.natural_key == '':
                    src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                    stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                    src_v = Table.get_instance(_key=(self.src_v_schema.id, row.table_name))
                    src_lv = LayerTable.get_instance(_key=(self.src_layer.id, src_v.id))

                    pipeline = Pipeline.get_instance(_key=src_lv.id)
                    src_col = Column.get_instance(_key=(src_table.id, row.column_name))
                    stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                    if src_col and stg_col:
                        ColumnMapping(pipeline_id=pipeline.id
                                      , col_seq=0
                                      , src_col_id=src_col.id
                                      , tgt_col_id=stg_col.id
                                      , src_col_trx=row.column_transformation_rule
                                      )
                    else:
                        logging.error(f"Invalid column '{row.column_name}', processing row:\n{row}")

            @log_error_decorator()
            def extract_stg_view_columns(row):
                if row.natural_key == '':
                    stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                    stg_col = Column.get_instance(_key=(stg_t.id, row.column_name))

                    stg_v = Table.get_instance(_key=(self.stg_v_schema.id, row.table_name))
                    stg_lv = LayerTable.get_instance(_key=(self.stg_layer.id, stg_v.id))

                    pipeline = Pipeline.get_instance(_key=stg_lv.id)

                    if stg_col:
                        ColumnMapping(pipeline_id=pipeline.id
                                      , col_seq=0
                                      , src_col_id=stg_col.id
                                      , tgt_col_id=stg_col.id
                                      , src_col_trx=None
                                      )
                    else:
                        logging.error(f"Invalid column '{row.column_name}', processing row:\n{row}")

            @log_error_decorator()
            def extract_core_columns(row):
                core_table: Table
                core_table = Table.get_instance(_key=(self.core_t_schema.id, row.table_name))
                data_type_lst = row.data_type.split(sep='(')
                _data_type = data_type_lst[0]
                data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
                if data_type:

                    if core_table.history_table:
                        is_start_date = 1 if row.historization_key.upper() == 'S' else 0
                        is_end_date = 1 if row.historization_key.upper() == 'E' else 0
                        pk = 1 if (row.historization_key.upper() == 'Y' or is_start_date) else 0
                    else:
                        is_start_date = 0
                        is_end_date = 0
                        pk = 1 if row.pk.upper() == 'Y' else 0

                    mandatory = 1 if (row.mandatory.upper() == 'Y' or pk) else 0
                    precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None

                    # if is_start_date and not pk:
                    #     logging.error(f"Start date column '{row.column_name}', should be primary key as well!, processing row:\n{row}")

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
                surrogate_table: Table
                set_table: Table
                surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                set_table = Table.get_instance(_key=(self.core_t_schema.id, row.key_set_name))
                if set_table and surrogate_table:
                    DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                    surrogate_table.is_bkey = True
                else:
                    logging.error(f"Invalid set table {row.key_set_name} or surrogate table {row.physical_table}, processing row:\n {row}")

            @log_error_decorator()
            def extract_bkey_domains(row):
                data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
                if data_set and row.key_domain_id:
                    Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)
                else:
                    logging.error(f"invalid data set {row.key_set_id} or domain ID, processing row:\n{row}")

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
            def extract_bmap_datasets(row):
                set_table: Table
                surrogate_table: Table
                surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
                set_table = Table.get_instance(_key=(self.core_t_schema.id, row.code_set_name))
                if set_table and row.code_set_id and surrogate_table:
                    DataSet(set_type_id=self.bmap_set_type.id, set_code=row.code_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                    set_table.is_lkp = True
                    surrogate_table.is_bmap = True
                else:
                    logging.error(f"Invalid set table '{row.code_set_name}' or surrogate table '{surrogate_table}', processing row:\n{row}")

            @log_error_decorator()
            def extract_bmap_domains(row):
                data_set = DataSet.get_instance(_key=(self.bmap_set_type.id, row.code_set_id))
                Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)

            @log_error_decorator()
            def extract_bmap_values(row):
                ds: DataSet
                data_set_lst = [ds for ds in bmaps_data_sets if ds.set_table.table_name == row.code_set_name.upper()]
                if len(data_set_lst) > 0:
                    data_set = data_set_lst[0]
                    domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
                    if domain:
                        DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

            @log_error_decorator()
            def extract_bkey_txf_views(row):
                if row.natural_key != '' and row.key_domain_name != '':
                    ds = DataSource.get_instance(_key=row.schema)
                    if not ds:
                        logging.error(f"Invalid source name '{row.schema}', processing row:\n{row}")
                    else:
                        domain: Domain
                        stg_col: Column
                        stg_t: Table
                        stg_lt: LayerTable
                        srci_col: Column

                        data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                        if not data_set:
                            logging.error(f"Invalid key set '{row.key_set_name}', processing row:\n{row}")
                        else:
                            domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                            if not domain:
                                logging.error(f"Invalid key domain name '{row.key_domain_name}', processing row:\n{row}")
                            else:
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

                                bkey_v = Table(schema_id=self.txf_bkey_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)
                                bkey_vl = LayerTable(layer_id=self.txf_bkey_layer.id, table_id=bkey_v.id)

                                bkey_pipeline = Pipeline(src_lyr_table_id=stg_lt.id
                                                         , tgt_lyr_table_id=domain.data_set.surrogate_table.id
                                                         , lyr_view_id=bkey_vl.id
                                                         , domain_id=srci_col.domain.id)
                                if row.bkey_filter:
                                    Filter(pipeline_id=bkey_pipeline.id, filter_seq=1, complete_filter_expr=row.bkey_filter)

            @log_error_decorator()
            def extract_srci_view_columns(row):
                stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))
                stg_t_col = None

                srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                srci_t_col = Column.get_instance(_key=(srci_t.id, row.column_name))

                if row.natural_key != '':
                    if row.key_set_name != '':
                        data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                        if not data_set:
                            logging.error(f"key set '{row.key_set_name}', is not defined, please check the 'BKEY' sheet!, processing row:\n{row}")
                        else:
                            domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                            if not domain:
                                logging.error(f"""{row.key_domain_name}, is not defined, please check the 'BKEY' sheet!, processing row:\n{row}""")
                            else:
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
                    if not stg_t_col:
                        logging.error(f"Invalid column {row.column_name}, processing row:\n{row}")

                if not srci_t_col:
                    logging.error(f"Invalid column {row.column_name}, processing row:\n{row}")
                else:
                    srci_v = Table.get_instance(_key=(self.srci_v_schema.id, row.table_name))
                    srci_vl = LayerTable.get_instance(_key=(self.srci_layer.id, srci_v.id))

                    srci_v_pipeline = Pipeline.get_instance(_key=srci_vl.id)
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
                    if (' select ' or ' sel ' or '(sel ' or '(select ') in _join_txt:
                        logging.error(f"Query cannot be found in join!, processing row:\n{row}")
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

                        if not join_type:
                            logging.error(f"No join found!, processing row:\n{row}")

                        _split = _split[1].split(' on ', 1)
                        # print('on split: ', _split)
                        if len(_split) >= 2:
                            _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
                            _split_1 = _split[1]

                            table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
                            table_name = table__alias[0]
                            table_alias = table__alias[1] if len(table__alias) >= 2 else table_name
                            with_srci_t = Table.get_instance(_key=(self.srci_t_schema.id, table_name))

                            if not with_srci_t:
                                logging.error(f"invalid join table '{table_name}', processing row:\n{row}")
                            else:
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

                    tgt_col: Column
                    transformation_type = _row.transformation_type.upper()
                    if transformation_type not in ('COPY', 'SQL', 'CONST'):
                        logging.error(f"Transformation Type, should be one of the following COPY, SQL or CONST, processing row:\n{_row}")
                    else:
                        tgt_col = Column.get_instance(_key=(core_t.id, _row.column_name))
                        if not tgt_col:
                            logging.error(f"Invalid Target Column Name, {_row.column_name}, processing row:\n{_row}")
                        else:
                            src_col = None
                            src_table_alias = None
                            src_table: Table
                            scd_type = 1
                            if row.historization_algorithm.upper() == 'INSERT':
                                scd_type = 0
                            elif row.historization_algorithm.upper() in ('UPSERT', 'UPSERTDELETE'):
                                scd_type = 1
                            elif row.historization_algorithm.upper() == 'HISTORY':
                                scd_type = 2 if _row.column_name in row.historization_columns else 1
                            else:
                                logging.error(f"Invalid historization algorithm, processing row:\n{row}")

                            scd_type = 0 if tgt_col.is_pk else scd_type

                            if transformation_type == 'COPY':
                                transformation_rule = None
                                if _row.mapped_to_column:
                                    src_table_alias = _row.mapped_to_table
                                    src_table = core_pipeline.get_table_by_alias(src_table_alias)
                                    if src_table:
                                        src_t = Table.get_instance(_key=(self.srci_t_schema.id, src_table.table_name))
                                        if src_t:
                                            src_col = Column.get_instance(_key=(src_t.id, _row.mapped_to_column))
                                        else:
                                            logging.error(f"Invalid Table '{src_table.table_name}', processing row:\n{_row}")
                                    else:
                                        logging.error(f"Invalid alias '{src_table_alias}', processing row:\n{_row}")
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
                ds = DataSource.get_instance(_key=row.source)
                if not ds:
                    logging.error(f"Invalid source name {row.source}, processing row:\n{row}")
                else:
                    main_tables_name = row.main_source.split(',')
                    main_table_name = main_tables_name[0].strip()
                    main_table_alias = row.main_source_alias if row.main_source_alias else main_table_name

                    srci_t = Table.get_instance(_key=(self.srci_t_schema.id, main_table_name))
                    if not srci_t:
                        logging.error(f"invalid main table, {main_table_name}, processing row:\n{row}")
                    else:
                        srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))

                        core_t = Table.get_instance(_key=(self.core_t_schema.id, row.target_table_name))
                        core_lt = LayerTable.get_instance(_key=(self.core_layer.id, core_t.id))

                        # if (row.historization_algorithm.upper() == 'HISTORY' and not core_t.history_table) \
                        #         or (row.historization_algorithm.upper() != 'HISTORY' and core_t.history_table):
                        #     logging.error(f"Historization algorithm and core table definition are not consistent, processing row:\n{row}")

                        txf_view_name = CORE_VIEW_NAME_TEMPLATE.format(mapping_name=row.mapping_name)
                        core_txf_v = Table(schema_id=self.txf_core_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)

                        core_txf_vl = LayerTable(layer_id=self.txf_core_layer.id, table_id=core_txf_v.id)
                        core_pipeline = Pipeline(src_lyr_table_id=srci_lt.id
                                                 , tgt_lyr_table_id=core_lt.id
                                                 , src_table_alias=main_table_alias
                                                 , lyr_view_id=core_txf_vl.id)

                        if row.join:
                            parse_join(row.join)

                        if row.filter_criterion:
                            Filter(pipeline_id=core_pipeline.id, filter_seq=1, complete_filter_expr=row.filter_criterion)

                        if 'column_mapping' in self.data.keys():
                            column_mapping_df = filter_dataframe(self.data['column_mapping'], 'mapping_name', row.mapping_name)
                            if not column_mapping_df.empty:
                                column_mapping_df[['column_name'
                                    , 'mapped_to_table'
                                    , 'mapped_to_column'
                                    , 'transformation_rule'
                                    , 'transformation_type']].drop_duplicates().apply(column_mapping, axis=1)
                            else:
                                logging.error(f"No column mapping found for {row.mapping_name}, processing row:\n{row}")

            ####################################################  Begin DFs  ####################################################
            if ('system' or 'stg_tables') not in self.data.keys():
                logging.error("Make sure 'System' and 'Stg Tables' sheets are both exists!")
            else:
                _core_tables = []
                _core_tables_bkey = []
                _core_tables_bmap = []
                _source_names = None
                history_tables_lst = []
                if source_name:
                    _source_names = source_name if isinstance(source_name, list) else [source_name]
                    _source_names.extend(UNIFIED_SOURCE_SYSTEMS)

                system_df = filter_dataframe(self.data['system'], 'schema', _source_names)
                stg_tables_df = filter_dataframe(self.data['stg_tables'], 'schema', _source_names)

                if system_df.empty or stg_tables_df.empty:
                    logging.error("Please make sure there data in both 'System' and 'Stg Tables' sheets!")
                else:
                    table_mapping_df = pd.DataFrame()
                    if 'table_mapping' in self.data.keys():
                        table_mapping_df = filter_dataframe(filter_dataframe(self.data['table_mapping'], 'source', _source_names), 'layer', 'CORE')
                        _core_tables = [i for i in set(table_mapping_df['target_table_name'].values.tolist()) if i]

                        history_core_tables_df = filter_dataframe(table_mapping_df, 'historization_algorithm', 'HISTORY')
                        if not history_core_tables_df.empty:
                            history_tables_lst = history_core_tables_df['target_table_name'].drop_duplicates().tolist()

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

                    if 'core_tables' in self.data.keys():
                        core_tables_df = filter_dataframe(self.data['core_tables'], 'table_name', all_core_tables)

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
                        core_tables_df[['table_name', 'column_name', 'data_type', 'pk'
                            , 'mandatory', 'historization_key']].drop_duplicates().apply(extract_core_columns, axis=1)
                        table: Table
                        col: Column
                        invalid_history_tables = []
                        invalid_lookup_tables = []
                        for table in Table.get_all_instances():
                            if table.history_table:
                                if not (table.key_col and table.start_date_col and table.end_date_col):
                                    invalid_history_tables.append(table.table_name)
                            elif table.is_lkp:
                                col_names = [col.column_name for col in table.columns]
                                if f"{table.table_name}_CD" not in col_names:
                                    invalid_lookup_tables.append(table.table_name)
                                elif f"{table.table_name}_DESC" not in col_names:
                                    invalid_lookup_tables.append(table.table_name)

                        if invalid_history_tables:
                            logging.error(f"These are invalid history tables, {invalid_history_tables}")

                        if invalid_lookup_tables:
                            logging.error(f"These are invalid lookup tables, {invalid_lookup_tables}.\nCODE & DESC columns should follow the pattern <table_name>_CD & <table_name>_DESC")

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
                    myid_summary = "\n\nSummary:\n######################\n\n"
                    for class_name in MyID.get_all_classes_instances().keys():
                        cls_instances_cout = eval(f"{class_name}.count_instances()")
                        class_count = f'{class_name} count: {cls_instances_cout}\n'
                        myid_summary += class_count

                    logging.info(myid_summary)

                    # MyID.serialize_all()

        extract_all()


@log_error_decorator()
def layer_table_scripts(row):
    row.layer_table: LayerTable
    ddl = row.layer_table.table.ddl
    if ddl:
        kind_folder = 'tables' if row.layer_table.table.table_kind == 'T' else 'views'
        tables_file = WriteFile(row.out_path, kind_folder, "sql", 'a')
        tables_file.write(ddl)
        tables_file.write('\n')
        tables_file.close()

    dml = None
    if row.layer_table.table.is_bmap:
        dml = row.layer_table.dml
    elif row.layer_table.table.is_lkp:
        schema_name = row.layer_table.table.schema.schema_name
        table_name = row.layer_table.table.table_name
        core_lookup_ds = row.layer_table.core_lookup_ds
        if core_lookup_ds:
            src_schema = core_lookup_ds.surrogate_table.schema.schema_name
            src_table = core_lookup_ds.surrogate_table.table_name
            dml = f"""insert into {schema_name}.{table_name}\n({table_name}_CD, {table_name}_DESC)\nselect distinct EDW_CODE,DESCRIPTION\nfrom {src_schema}.{src_table};\n"""

    if dml:
        data_file = WriteFile(row.out_path, 'data', "sql", 'a')
        data_file.write(dml)
        data_file.write('\n')
        data_file.close()


@log_error_decorator()
def generate_schemas_ddl(smx: SMX):
    db_file = WriteFile(smx.current_scripts_path, "schemas", "sql")
    for schema in Schema.get_all_instances():
        ddl = schema.ddl
        if ddl:
            db_file.write(ddl)
            db_file.write("\n")
    db_file.close()


@log_error_decorator()
@time_elapsed_decorator
def generate_scripts(smx: SMX):
    source_dict: dict = {}
    core_model_path = os.path.join(smx.current_scripts_path, CORE_MODEL_FOLDER_NAME)
    src_systems_path = os.path.join(smx.current_scripts_path, SRC_SYSTEMS_FOLDER_NAME)

    # create_folder(core_model_path)

    for src in smx.source_systems:
        source_path = os.path.join(smx.current_scripts_path, src_systems_path, src)
        source_dict[src.upper()] = source_path
        # create_folder(source_path)
        del source_path

    def layer_table_out_path(row):
        layer_table: LayerTable
        layer_table = row.layer_table

        if layer_table.table.data_source:
            main_folder = source_dict[layer_table.table.data_source.source_name]
        else:
            main_folder = core_model_path

        layer_folder_name = f"Layer_{layer_table.layer.layer_level}_{layer_table.layer.layer_name}"

        path = os.path.join(smx.current_scripts_path, main_folder, layer_folder_name)
        return path

    layer_tables_df = pd.DataFrame(LayerTable.get_all_instances(), columns=['layer_table'])
    layer_tables_df['out_path'] = layer_tables_df.apply(layer_table_out_path, axis=1)
    layer_tables_df[['out_path']].drop_duplicates().apply(lambda row: create_folder(row.out_path), axis=1)

    if not layer_tables_df.empty:
        # print('start generating scripts!')
        layer_tables_df.apply(layer_table_scripts, axis=1)
        # layer_tables_df.swifter.apply(layer_table_scripts, axis=1)
        # layer_tables_df.parallel_apply(layer_table_scripts, axis=1)


@log_error_decorator()
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
    core_tables_lkp = WriteFile(smx.metadata_scripts, 'core_tables_lkp', "sql")
    gcfr_transform_keycol = WriteFile(smx.metadata_scripts, 'gcfr_transform_keycol', "sql")
    history = WriteFile(smx.metadata_scripts, 'history', 'sql')
    etl_process = WriteFile(smx.metadata_scripts, 'etl_process', "sql")

    source: DataSource
    table: Table
    pk_col: Column
    hist_col_mapping: ColumnMapping
    pipeline: Pipeline

    for source in DataSource.get_all_instances():
        src_name_lkp.write(INSERT_INTO_SOURCE_NAME_LKP.format(meta_db=smx.meta_v_schema.schema_name,
                                                              SOURCE_NAME=single_quotes(source.source_name),
                                                              rejection_table_name='NULL',
                                                              business_rules_table_name='NULL',
                                                              LOADING_TYPE=single_quotes('ONLINE'),
                                                              SOURCE_DB=single_quotes(smx.src_v_schema.schema_name),
                                                              DATA_SRC_CD='NULL'
                                                              )
                           )
        for table in source.tables:
            if table.schema.id == smx.src_v_schema.id:
                src_tables_lkp.write(INSERT_INTO_SOURCE_TABLES_LKP.format(meta_db=smx.meta_v_schema.schema_name,
                                                                          SOURCE_NAME=single_quotes(source.source_name),
                                                                          TABLE_NAME=single_quotes(table.table_name),
                                                                          TRANSACTION_DATA=1 if table.transactional_data else 0
                                                                          )
                                     )
    for table in Table.get_all_instances():
        for pk_col in table.key_col:
            gcfr_transform_keycol.write(INSERT_INTO_GCFR_TRANSFORM_KEYCOL.format(meta_db=smx.meta_v_schema.schema_name,
                                                                                 OUT_DB_NAME=single_quotes(table.schema.schema_name),
                                                                                 OUT_OBJECT_NAME=single_quotes(table.table_name),
                                                                                 KEY_COLUMN=single_quotes(pk_col.column_name)
                                                                                 )
                                        )
        if table.schema.id == smx.core_t_schema.id:
            core_tables_lkp.write(INSERT_INTO_CORE_TABLES_LKP.format(meta_db=smx.meta_v_schema.schema_name,
                                                                     TABLE_NAME=table.table_name,
                                                                     IS_LOOKUP=1 if table.is_lkp else 0,
                                                                     IS_HISTORY=1 if table.history_table else 0,
                                                                     START_DATE_COLUMN=table.start_date_col.column_name if table.history_table else 'NULL',
                                                                     END_DATE_COLUMN=table.end_date_col.column_name if table.history_table else 'NULL'
                                                                     )
                                  )

    for pipeline in Pipeline.get_all_instances():
        if pipeline.tgt_lyr_table:
            process_type = None
            apply_type = 'INSERT'
            key_set_id = 'NULL'
            domain_id = 'NULL'
            if pipeline.lyr_view.layer.id == smx.txf_bkey_layer.id:
                process_type = 'BKEY'
                key_set_id = pipeline.tgt_lyr_table.table.surrogate_data_set.set_code
                domain_id = pipeline.domain.domain_code
            elif pipeline.lyr_view.layer.id == smx.txf_core_layer.id:
                process_type = 'TXF'
                if pipeline.scd_type2_col:
                    apply_type = 'HISTORY'
                elif pipeline.scd_type1_col:
                    apply_type = 'UPSERTDELETE'

            if pipeline.tgt_lyr_table.table.history_table:
                for hist_col_mapping in pipeline.scd_type2_col:
                    history.write(INSERT_INTO_HISTORY.format(meta_db=smx.meta_v_schema.schema_name,
                                                             PROCESS_NAME=single_quotes(pipeline.lyr_view.table.table_name),
                                                             HISTORY_COLUMN=single_quotes(hist_col_mapping.tgt_col.column_name)
                                                             )
                                  )
            if process_type:
                etl_process.write(INSERT_INTO_ETL_PROCESS.format(meta_db=smx.meta_v_schema.schema_name,
                                                                 SOURCE_NAME=single_quotes(pipeline.src_lyr_table.table.data_source.source_name),
                                                                 PROCESS_TYPE=single_quotes(process_type),
                                                                 PROCESS_NAME=single_quotes(pipeline.lyr_view.table.table_name),
                                                                 BASE_TABLE=single_quotes(pipeline.tgt_lyr_table.table.table_name),
                                                                 APPLY_TYPE=single_quotes(apply_type),
                                                                 INPUT_VIEW_DB=single_quotes(pipeline.lyr_view.table.schema.schema_name),
                                                                 TARGET_TABLE_DB=single_quotes(pipeline.tgt_lyr_table.table.schema.schema_name),
                                                                 TARGET_VIEW_DB='NULL',
                                                                 SRCI_TABLE_DB=single_quotes(pipeline.src_lyr_table.table.schema.schema_name),
                                                                 SRCI_TABLE_NAME=single_quotes(pipeline.src_lyr_table.table.table_name),
                                                                 KEY_SET_ID=key_set_id,
                                                                 DOMAIN_ID=domain_id,
                                                                 CODE_SET_ID='NULL'
                                                                 )
                                  )

    src_name_lkp.close()
    src_tables_lkp.close()
    gcfr_transform_keycol.close()
    etl_process.close()
    history.close()
    core_tables_lkp.close()


@time_elapsed_decorator
def deploy(scripts_path):
    pass
