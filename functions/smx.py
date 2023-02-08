from functions.model import *


class SMX:
    SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
        , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping'
        , 'supplements']

    LayerDtl = namedtuple("LayerDetail", "level v_db t_db")
    LAYERS = {'SRC': LayerDtl(0, 'GDEV1V_STG_ONLINE', 'STG_ONLINE')
        , 'STG': LayerDtl(1, 'GDEV1V_STG', 'GDEV1T_STG')
        , 'TXF_BKEY': LayerDtl(2, 'GDEV1V_INP', '')
        , 'BKEY': LayerDtl(3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
        , 'BMAP': LayerDtl(3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
        , 'SRCI': LayerDtl(4, 'GDEV1V_SRCI', 'GDEV1T_SRCI')
        , 'TXF_CORE': LayerDtl(5, 'GDEV1V_INP', '')
        , 'CORE': LayerDtl(6, 'GDEV1V_BASE', 'GDEV1T_BASE')}

    @time_elapsed_decorator
    def __init__(self, smx_path, scripts_path):
        self.path = smx_path
        self.scripts_path = scripts_path
        self.run_id = str(generate_run_id())
        self.current_scripts_path = os.path.join(self.scripts_path, self.run_id)
        self.log_error_path = self.current_scripts_path
        create_folder(self.current_scripts_path)
        self.xls = None
        self.reserved_words = {}
        self.data = {}

        for layer_key, layer_value in self.LAYERS.items():
            Layer(layer_name=layer_key, abbrev=layer_key, layer_level=layer_value.level)
            Schema(schema_name=layer_value.t_db, _override=1)
            Schema(schema_name=layer_value.v_db, _override=1)

        DataSetType(set_type='BKEY')
        DataSetType(set_type='BMAP')

        self.bkey_set_type = DataSetType.get_instance(_key='bkey')
        self.bmap_set_type = DataSetType.get_instance(_key='BMAP')

        self.src_layer = Layer.get_instance(_key='SRC')
        self.stg_layer = Layer.get_instance(_key='STG')
        self.bkey_layer = Layer.get_instance(_key='BKEY')
        self.srci_layer = Layer.get_instance(_key='SRCI')
        self.core_layer = Layer.get_instance(_key='CORE')

        self.src_t_schema = Schema.get_instance(_key=self.LAYERS['SRC'].t_db)
        self.stg_t_schema = Schema.get_instance(_key=self.LAYERS['STG'].t_db)
        self.utlfw_t_schema = Schema.get_instance(_key=self.LAYERS['BKEY'].t_db)
        self.srci_t_schema = Schema.get_instance(_key=self.LAYERS['SRCI'].t_db)
        self.core_t_schema = Schema.get_instance(_key=self.LAYERS['CORE'].t_db)

        self.src_v_schema = Schema.get_instance(_key=self.LAYERS['SRC'].v_db)
        self.stg_v_schema = Schema.get_instance(_key=self.LAYERS['STG'].v_db)
        self.utlfw_v_schema = Schema.get_instance(_key=self.LAYERS['BKEY'].v_db)
        self.srci_v_schema = Schema.get_instance(_key=self.LAYERS['SRCI'].v_db)
        self.core_v_schema = Schema.get_instance(_key=self.LAYERS['CORE'].v_db)

    @time_elapsed_decorator
    def parse_file(self):
        self.xls = pd.ExcelFile(self.path)
        for sheet in self.xls.sheet_names:
            self.parse_sheet(sheet)

        reserved_words_df = self.data['supplements']
        for key in reserved_words_df['reserved_words_source'].drop_duplicates():
            self.reserved_words[key] = reserved_words_df[reserved_words_df['reserved_words_source'] == key]['reserved_words'].unique().tolist()

    def parse_sheet(self, sheet):
        sheet_name = sheet.replace('  ', ' ').replace(' ', '_').lower()
        if sheet_name in self.SHEETS:
            df = self.xls.parse(sheet).replace(np.nan, value='', regex=True)
            df = df.applymap(lambda x: x.replace('\ufeff', '').strip() if type(x) is str else int(x) if type(x) is float else x)
            df.drop_duplicates()
            df.columns = [c.replace('  ', ' ').replace(' ', '_').lower() for c in df]
            self.data[sheet_name] = df

    @property
    def sheet_names(self):
        return self.data.keys()

    def clean_transformations(self, trx: str, extra: [] = None) -> str:
        sep = ''
        if extra is not None:
            words = self.reserved_words['TERADATA'] + extra
        else:
            words = self.reserved_words['TERADATA']

        sorted_words = sorted(words, key=len, reverse=True)
        for word in sorted_words:
            trx = trx.lower().replace(word.lower(), sep)

        for spc in SPECIAL_CHARACTERS:
            trx = trx.replace(spc, sep)

        for no in NUMBERS:
            trx = trx.replace(str(no), sep)

        return trx.strip()

    @time_elapsed_decorator
    @log_error_decorator(None)
    def extract_all(self):

        @log_error_decorator(self.log_error_path)
        def extract_system(row):
            DataSource(source_name=row.schema, source_level=1, scheduled=1)

        @log_error_decorator(self.log_error_path)
        def extract_data_types(row):
            data_type_lst = row.data_type.split(sep='(')
            DataType(dt_name=data_type_lst[0], _override=1)

        @log_error_decorator(self.log_error_path)
        def extract_stg_tables(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds != {}, ds_error_msg

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
        def extract_stg_table_columns(row):
            data_type_error_msg = f"Data type {row.data_type} for {row.table_name}.{row.column_name} column is invalid, please check 'Stg tables' sheet! "
            bkey_dataset_error_msg = f"key set '{row.key_set_name}', is not defined"
            bmap_dataset_error_msg = f"code set '{row.code_set_name}', is not defined"

            src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
            stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))

            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=_data_type)

            assert data_type != {}, data_type_error_msg

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
                domain = {}
                domain_error_msg = f"Domain not found for {row.table_name}.{row.column_name} column, please check 'Stg tables' sheet!"
                if row.key_set_name != '' and row.key_domain_name != '':

                    data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                    assert data_set != {}, bkey_dataset_error_msg

                    domain = Domain.get_by_name(data_set.id, row.key_domain_name)
                    domain_error_msg = f"Bkey Domain not found for {row.table_name}.{row.column_name} column, " \
                                       f"\n key set name:'{row.key_set_name}'" \
                                       f"\n key domain name: '{row.key_domain_name}'" \
                                       f"\n please check 'Stg tables' sheet!"

                elif row.code_set_name != '' and row.code_domain_name != '':

                    data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                    assert data_set != {}, bmap_dataset_error_msg

                    domain = Domain.get_by_name(data_set.id, row.code_domain_name)
                    domain_error_msg = f"Bmap Domain not found for {row.table_name}.{row.column_name} column, " \
                                       f"\n code set name:'{row.code_set_name}'" \
                                       f"\n code domain name: '{row.code_domain_name}'" \
                                       f"\n please check 'Stg tables' sheet!"

                assert domain != {}, domain_error_msg
                domain_id = domain.id

            if srci_table:
                Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory
                       , data_type_id=data_type.id, dt_precision=precision, domain_id=domain_id)

        @log_error_decorator(self.log_error_path)
        def extract_src_views(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds != {}, ds_error_msg

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
            assert ds != {}, ds_error_msg

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
                assert ds != {}, ds_error_msg

                src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
                stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))

                src_lyr_table = LayerTable.get_instance(_key=(self.src_layer.id, src_table.id))
                stg_lyr_table = LayerTable.get_instance(_key=(self.stg_layer.id, stg_table.id))

                pipeline = Pipeline.get_instance(_key=(src_lyr_table.id, stg_lyr_table.id))
                src_col = Column.get_instance(_key=(src_table.id, row.column_name))
                stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                assert src_col != {}, col_error_msg

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
                assert ds != {}, ds_error_msg

                stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                stg_lyr_table = LayerTable.get_instance(_key=(self.stg_layer.id, stg_table.id))

                pipeline = Pipeline.get_instance(_key=(stg_lyr_table.id, stg_lyr_table.id))
                stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                assert stg_col != {}, col_error_msg

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
            data_type = DataType.get_instance(_key=_data_type)
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
            table = Table(schema_id=self.utlfw_t_schema.id, table_name=row.physical_table, table_kind='T')
            LayerTable(layer_id=self.bkey_layer.id, table_id=table.id)

            int_data_type = DataType.get_instance(_key='INTEGER')
            vchar_data_type = DataType.get_instance(_key='VARCHAR')

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
            table = Table.get_instance(_key=(self.utlfw_t_schema.id, row.physical_table))
            DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_name=row.key_set_name, table_id=table.id)

        @log_error_decorator(self.log_error_path)
        def extract_bkey_domains(row):
            data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
            Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)

        @log_error_decorator(self.log_error_path)
        def extract_bmap_tables(row):
            Table(schema_id=self.utlfw_t_schema.id, table_name=row.physical_table, table_kind='T')

        @log_error_decorator(self.log_error_path)
        def extract_bmap_datasets(row):
            table = Table.get_instance(_key=(self.utlfw_t_schema.id, row.physical_table))
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
        def extract_srci_views(row):
            ds_error_msg = f"""{row.schema}, is not defined, please check the 'System' sheet!"""
            ds = DataSource.get_instance(_key=row.schema)
            assert ds != {}, ds_error_msg

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

            stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            stg_lyr_table = LayerTable.get_instance(_key=(self.stg_layer.id, stg_table.id))

            srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
            srci_lyr_table = LayerTable.get_instance(_key=(self.srci_layer.id, srci_table.id))

            pipeline = Pipeline.get_instance(_key=(stg_lyr_table.id, srci_lyr_table.id))

            stg_t_col = Column.get_instance(_key=(stg_table.id, row.column_name))
            srci_t_col = Column.get_instance(_key=(srci_table.id, row.column_name))

            if row.natural_key != '':
                weird_txt = self.clean_transformations(row.natural_key, extra=[col.column_name for col in stg_table.columns])
                trx_error_msg = f'invalid transformation for {row.schema}, {row.table_name}.{row.column_name} ' \
                                f'\n-> {row.natural_key} ' \
                                f'\n-xx->{weird_txt}<-xx-'

                assert weird_txt == '' or weird_txt == 'null', trx_error_msg

            assert stg_t_col != {}, col_error_msg
            assert srci_t_col != {}, col_error_msg

            ColumnMapping(pipeline_id=pipeline.id
                          , col_seq=0
                          , src_col_id=stg_t_col.id
                          , tgt_col_id=srci_t_col.id
                          , src_col_trx=None
                          )

        self.data['system'].drop_duplicates().apply(extract_system, axis=1)
        self.data['stg_tables'][['data_type']].drop_duplicates().apply(extract_data_types, axis=1)
        self.data['core_tables'][['data_type']].drop_duplicates().apply(extract_data_types, axis=1)
        self.reserved_words['TERADATA'] = list((self.reserved_words['TERADATA'] + [dt.dt_name for dt in DataType.get_instance()]))
        ########################## Start bkey & bmaps #####################
        self.data['bkey'][['physical_table']].drop_duplicates().apply(extract_bkey_tables, axis=1)
        self.data['bkey'][['key_set_name', 'key_set_id', 'physical_table']].drop_duplicates().apply(extract_bkey_datasets, axis=1)
        self.data['bkey'][['key_set_id', 'key_domain_id', 'key_domain_name']].drop_duplicates().apply(extract_bkey_domains, axis=1)

        self.data['bmap'][['physical_table']].drop_duplicates().apply(extract_bmap_tables, axis=1)
        self.data['bmap'][['code_set_name', 'code_set_id', 'physical_table']].drop_duplicates().apply(extract_bmap_datasets, axis=1)
        bmaps_data_sets = DataSetType.get_instance(_key='BMAP').data_sets
        self.data['bmap'][['code_set_id', 'code_domain_id', 'code_domain_name']].drop_duplicates().apply(extract_bmap_domains, axis=1)
        self.data['bmap_values'][['code_set_name', 'code_domain_id', 'edw_code', 'source_code', 'description']].drop_duplicates().apply(extract_bmap_values, axis=1)
        ########################## End bkey & bmaps #####################

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_stg_tables, axis=1)
        self.data['stg_tables'][['table_name', 'column_name', 'data_type', 'mandatory', 'natural_key', 'pk',
                                 'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name']].drop_duplicates().apply(extract_stg_table_columns, axis=1)

        self.data['core_tables'][['table_name']].drop_duplicates().apply(extract_core_tables, axis=1)
        self.data['core_tables'][['table_name', 'column_name', 'data_type', 'pk', 'mandatory', 'historization_key']].drop_duplicates().apply(extract_core_columns, axis=1)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_src_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'natural_key', 'column_name', 'column_transformation_rule']].drop_duplicates().apply(extract_src_view_columns, axis=1)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_stg_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'natural_key', 'column_name']].drop_duplicates().apply(extract_stg_view_columns, axis=1)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(extract_srci_views, axis=1)
        self.data['stg_tables'][['schema', 'table_name', 'column_name', 'natural_key']].drop_duplicates().apply(extract_srci_view_columns, axis=1)

        print('DataSetType count:', len(DataSetType.get_instance()))
        print('Layer count:', len(Layer.get_instance()))
        print('Schema count:', len(Schema.get_instance()))
        print('DataType count:', len(DataType.get_instance()))
        print('DataSource count:', len(DataSource.get_instance()))
        print('Table count:', len(Table.get_instance()))
        print('LayerTable count:', len(LayerTable.get_instance()))
        print('DataSet count:', len(DataSet.get_instance()))
        print('Domain count:', len(Domain.get_instance()))
        print('DomainValue count:', len(DomainValue.get_instance()))
        print('Column count:', len(Column.get_instance()))
        print('Pipeline count:', len(Pipeline.get_instance()))
        print('ColumnMapping count:', len(ColumnMapping.get_instance()))

    @time_elapsed_decorator
    def generate_scripts(self):
        def layer_scripts(layer: Layer):
            def layer_table_scripts(layer_table: LayerTable):
                ddl = layer_table.table.ddl
                if layer_table.table.table_kind == 'T':
                    tables_ddl.append(ddl)
                elif layer_table.table.table_kind == 'V':
                    views_ddl.append(ddl)

            layer_folder_name = f"Layer_{layer.layer_level}_{layer.layer_name}"
            layer_path = os.path.join(self.current_scripts_path, layer_folder_name)
            create_folder(layer_path)

            tables_ddl = []
            views_ddl = []
            threads(layer_table_scripts, layer.layer_tables)
            # for layer_table in layer.layer_tables:

            tables_ddl = list(filter(None, tables_ddl))
            views_ddl = list(filter(None, views_ddl))

            tables_ddl = "\n".join(tables_ddl)
            views_ddl = "\n".join(views_ddl)

            ################ START WRITE TO FILES ################
            tables_file = WriteFile(layer_path, 'tables', "sql")
            tables_file.write(tables_ddl)
            tables_file.close()

            views_file = WriteFile(layer_path, 'views', "sql")
            views_file.write(views_ddl)
            views_file.close()

        threads(layer_scripts, Layer.get_instance())
        open_folder(self.current_scripts_path)
