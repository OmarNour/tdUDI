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
            Schema(schema_name=layer_value.t_db)
            Schema(schema_name=layer_value.v_db)

        DataSetType(set_type='BKEY')
        DataSetType(set_type='BMAP')

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
            df = df.applymap(lambda x: x.strip() if type(x) is str else int(x) if type(x) is float else x)
            df.drop_duplicates()
            df.columns = [c.replace('  ', ' ').replace(' ', '_').lower() for c in df]
            self.data[sheet_name] = df

    @property
    def sheet_names(self):
        return self.data.keys()

    def extract_all(self):
        self.extract_data_sources()

        self.extract_bkeys()
        self.extract_bmaps()
        self.extract_bmap_values()
        self.extract_data_types()

        self.extract_staging_tables()
        self.extract_stg_tables_columns()

        self.extract_staging_views()
        self.extract_stg_views_columns()

        self.extract_core_tables()
        self.extract_core_columns()

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
    def extract_bmaps(self):
        def bmap_tables(row):
            Table(schema_id=utlfw_schema.id, table_name=row.physical_table, table_kind='T')

        def bmap(row):
            table = Table.get_instance(_key=(utlfw_schema.id, row.physical_table))
            DataSet(set_type_id=set_type.id, set_code=row.code_set_id, set_name=row.code_set_name, table_id=table.id)

        def domain(row):
            data_set = DataSet.get_instance(_key=(set_type.id, row.code_set_id))
            Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)

        utlfw_schema = Schema.get_instance(_key='gdev1t_utlfw')
        self.data['bmap'][['physical_table']].drop_duplicates().apply(bmap_tables, axis=1)

        set_type = DataSetType.get_instance(_key='bmap')
        self.data['bmap'][['code_set_name', 'code_set_id', 'physical_table']].drop_duplicates().apply(bmap, axis=1)
        self.data['bmap'][['code_set_id', 'code_domain_id', 'code_domain_name']].drop_duplicates().apply(domain, axis=1)

    @time_elapsed_decorator
    def extract_bmap_values(self):
        def bmap_values(row):
            data_set_lst = [ds for ds in bmaps_lst if ds.set_name == row.code_set_name]
            if len(data_set_lst) > 0:
                data_set = data_set_lst[0]
                domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
                if domain:
                    DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

        bmaps_lst = DataSetType.get_instance(_key='BMAP').data_sets

        self.data['bmap_values'][['code_set_name', 'code_domain_id', 'edw_code', 'source_code', 'description']].drop_duplicates().apply(bmap_values, axis=1)

    @time_elapsed_decorator
    def extract_bkeys(self):
        def bkey_tables(row):
            Table(schema_id=utlfw_schema.id, table_name=row.physical_table, table_kind='T')

        def bkey(row):
            table = Table.get_instance(_key=(utlfw_schema.id, row.physical_table))
            DataSet(set_type_id=set_type.id, set_code=row.key_set_id, set_name=row.key_set_name, table_id=table.id)

        def domain(row):
            data_set = DataSet.get_instance(_key=(set_type.id, row.key_set_id))
            Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)

        utlfw_schema = Schema.get_instance(_key='gdev1t_utlfw')
        self.data['bkey'][['physical_table']].drop_duplicates().apply(bkey_tables, axis=1)

        set_type = DataSetType.get_instance(_key='bkey')
        self.data['bkey'][['key_set_name', 'key_set_id', 'physical_table']].drop_duplicates().apply(bkey, axis=1)
        self.data['bkey'][['key_set_id', 'key_domain_id', 'key_domain_name']].drop_duplicates().apply(domain, axis=1)

    @time_elapsed_decorator
    def extract_data_sources(self):
        self.data['system'].apply(lambda x: DataSource(source_name=x.schema, source_level=1, scheduled=1), axis=1)

    def extract_staging_views(self):
        """
        get Layer & Table then LayerTable to create Pipeline
        then create the ColumnMapping
        :return:
        """

        def stg_views(row):
            ds = DataSource.get_instance(_key=row.schema)
            if ds:
                src_t = Table.get_instance(_key=(src_t_schema.id, row.table_name))
                stg_t = Table.get_instance(_key=(stg_t_schema.id, row.table_name))

                src_lt = LayerTable.get_instance(_key=(src_layer.id, src_t.id))
                stg_lt = LayerTable.get_instance(_key=(stg_layer.id, stg_t.id))

                Pipeline(src_lyr_table_id=src_lt.id, tgt_lyr_table_id=stg_lt.id, schema_id=src_v_schema.id)
            #     src_v_ddl = DDL_VIEW_TEMPLATE.format(schema_name=src_v_schema.schema_name
            #                                          , view_name=row.table_name
            #                                          , query_txt=f"select * from {src_t_schema.schema_name}.{row.table_name}"
            #                                          )
            #     stg_v_ddl = DDL_VIEW_TEMPLATE.format(schema_name=stg_v_schema.schema_name
            #                                          , view_name=row.table_name
            #                                          , query_txt=f"select * from {stg_t_schema.schema_name}.{row.table_name}"
            #                                          )
            #
            #     src_v = Table(schema_id=src_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id, ddl=src_v_ddl)
            #     stg_v = Table(schema_id=stg_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id, ddl=stg_v_ddl)
            #
            #     LayerTable(layer_id=src_layer.id, table_id=src_v.id)
            #     LayerTable(layer_id=stg_layer.id, table_id=stg_v.id)

        src_layer = Layer.get_instance(_key='SRC')
        stg_layer = Layer.get_instance(_key='STG')

        src_v_schema = Schema.get_instance(_key=self.LAYERS['SRC'].v_db)
        src_t_schema = Schema.get_instance(_key=self.LAYERS['SRC'].t_db)

        stg_t_schema = Schema.get_instance(_key=self.LAYERS['STG'].t_db)
        # stg_v_schema = Schema.get_instance(_key=self.LAYERS['STG'].v_db)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(stg_views, axis=1)

    @time_elapsed_decorator
    def extract_staging_tables(self):
        def stg_tables(row):
            ds = DataSource.get_instance(_key=row.schema)
            if ds:
                src_t = Table(schema_id=src_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                stg_t = Table(schema_id=stg_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)

                LayerTable(layer_id=src_layer.id, table_id=src_t.id)
                LayerTable(layer_id=stg_layer.id, table_id=stg_t.id)

        src_layer = Layer.get_instance(_key='SRC')
        stg_layer = Layer.get_instance(_key='STG')

        src_t_schema = Schema.get_instance(_key=self.LAYERS['SRC'].t_db)
        stg_t_schema = Schema.get_instance(_key=self.LAYERS['STG'].t_db)

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(stg_tables, axis=1)

    @time_elapsed_decorator
    def extract_core_tables(self):
        def core_tables(row):
            core_t = Table(schema_id=core_schema.id, table_name=row.table_name, table_kind='T')
            LayerTable(layer_id=core_layer.id, table_id=core_t.id)

        core_layer = Layer.get_instance(_key='CORE')
        core_schema = Schema.get_instance(_key='gdev1t_base')
        self.data['core_tables'][['table_name']].drop_duplicates().apply(core_tables, axis=1)

    @time_elapsed_decorator
    def extract_data_types(self):
        def data_types(row):
            data_type_lst = row.data_type.split(sep='(')
            DataType(dt_name=data_type_lst[0])

        self.data['stg_tables'][['data_type']].drop_duplicates().apply(data_types, axis=1)

    @time_elapsed_decorator
    def extract_stg_views_columns(self):
        @log_error_decorator(self.log_error_path)
        def stg_columns(row):
            ds_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
            tbl_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
            col_error_msg = f'{row.schema}, {row.table_name}.{row.column_name} has no object defined!'
            if row.natural_key == '':
                ds = DataSource.get_instance(_key=row.schema)
                if ds:
                    src_table = Table.get_instance(_key=(src_t_schema.id, row.table_name))
                    stg_table = Table.get_instance(_key=(stg_t_schema.id, row.table_name))

                    src_lyr_table = LayerTable.get_instance(_key=(src_layer.id, src_table.id))
                    stg_lyr_table = LayerTable.get_instance(_key=(stg_layer.id, stg_table.id))

                    pipeline = Pipeline.get_instance(_key=(src_lyr_table.id, stg_lyr_table.id))
                    src_col = Column.get_instance(_key=(src_table.id, row.column_name))
                    stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))

                    assert src_col is not None, col_error_msg

                    ColumnMapping(pipeline_id=pipeline.id
                                  , col_seq=0
                                  , src_col_id=src_col.id
                                  , tgt_col_id=stg_col.id
                                  , src_col_trx=row.column_transformation_rule
                                  )

        src_t_schema = Schema.get_instance(_key='stg_online')
        stg_t_schema = Schema.get_instance(_key='gdev1t_stg')

        src_layer = Layer.get_instance(_key='SRC')
        stg_layer = Layer.get_instance(_key='STG')

        self.data['stg_tables'][['schema', 'table_name', 'natural_key'
            , 'column_name', 'column_transformation_rule']].drop_duplicates().apply(stg_columns, axis=1)

    @time_elapsed_decorator
    def extract_stg_tables_columns(self):
        def stg_columns(row):
            src_table = Table.get_instance(_key=(src_t_schema.id, row.table_name))
            stg_table = Table.get_instance(_key=(stg_t_schema.id, row.table_name))
            srci_table = Table.get_instance(_key=(srci_t_schema.id, row.table_name))

            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=_data_type)
            if data_type:
                pk = 1 if row.pk.upper() == 'Y' else 0
                mandatory = 1 if row.pk.upper() == 'Y' else 0
                precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None

                if row.natural_key == '':
                    if src_table:
                        Column(table_id=src_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

                    if stg_table:
                        Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

                if srci_table:
                    Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)

        src_t_schema = Schema.get_instance(_key='stg_online')
        stg_t_schema = Schema.get_instance(_key='gdev1t_stg')
        srci_t_schema = Schema.get_instance(_key='gdev1t_srci')
        self.data['stg_tables'][['table_name', 'column_name', 'data_type'
            , 'mandatory', 'natural_key', 'pk'
                                 ]].drop_duplicates().apply(stg_columns, axis=1)

        # q = """select distinct table_name, column_name, data_type, pk , natural_key from df_stg_tables; """
        # df_stg_cols = sqldf(q, locals())
        # df_stg_cols.apply(stg_columns, axis=1)

    @time_elapsed_decorator
    def extract_core_columns(self):
        def core_columns(row):
            core_table = Table.get_instance(_key=(core_schema.id, row.table_name))
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

        core_schema = Schema.get_instance(_key='gdev1t_base')
        self.data['core_tables'][['table_name', 'column_name', 'data_type', 'pk', 'mandatory', 'historization_key']].drop_duplicates().apply(core_columns, axis=1)

    @time_elapsed_decorator
    def generate_scripts(self):
        layer: Layer
        for layer in Layer.get_instance():
            layer_folder_name = f"Layer_{layer.layer_level}_{layer.layer_name}"
            layer_path = os.path.join(self.current_scripts_path, layer_folder_name)
            create_folder(layer_path)

            tables_ddl = ''
            views_ddl = ''

            layer_table: LayerTable
            for layer_table in layer.layer_tables:
                ddl = layer_table.table.ddl
                if layer_table.table.table_kind == 'T':
                    tables_ddl += ddl if ddl else None
                elif layer_table.table.table_kind == 'V':
                    views_ddl += ddl if ddl else None

            tables_file = WriteFile(layer_path, 'tables', "sql")
            tables_file.write(tables_ddl)
            tables_file.close()

            views_file = WriteFile(layer_path, 'views', "sql")
            views_file.write(views_ddl)
            views_file.close()

        open_folder(self.current_scripts_path)
