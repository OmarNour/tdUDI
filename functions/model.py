from functions.functions import *


class Meta(type):
    """
    This is a custom metaclass that is used to generate unique IDs for instances of the MyID class.
    """

    cls_keys = {
        'server': 'server_name'
        , 'datasource': 'source_name'
        , 'schema': 'schema_name'
        , 'table': ('schema_id', 'table_name')
        , 'DataSetType': 'set_type'
        , 'DataSet': ('set_type_id', 'set_code')
        , 'Domain': ('data_set_id', 'domain_code')
        , 'DomainValue': ('domain_id', 'source_key')
        , 'Column': ('table_id', 'column_name')
        , 'DataType': 'data_type'
    }

    def __call__(cls, *args, **kwargs):
        """
        This method is called when an instance of the MyID class is created.
        It checks that named parameters are passed, generates a key from the named parameters and adds the key to the kwargs dictionary.
        """
        assert len(kwargs) > 0, f'Call {cls.__name__} with named parameters!'
        key = cls.__get_key(cls.__name__, **kwargs)
        kwargs.update({'_key': key})
        return super().__call__(*args, **kwargs)

    @staticmethod
    def _normalize_key(_key):
        """
        This method normalizes the key passed.
        """
        if isinstance(_key, tuple):
            return tuple(s.lower().strip() if isinstance(s, str) else s for s in _key)
        if isinstance(_key, str):
            return _key.lower().strip()
        return _key

    @classmethod
    def __get_key(cls, obj_name, **kwargs):
        """
        This method generates a key from the named parameters passed to the MyID class.
        """
        try:
            lower_cls_keys = {k.lower(): v for k, v in cls.cls_keys.items()}
            cls_key = lower_cls_keys[obj_name.lower()]

            if isinstance(cls_key, tuple):
                list_keys = []
                for key in cls_key:
                    key_value = kwargs.get(key)
                    assert key_value is not None, f"{key} not found!"
                    n_key_value = cls._normalize_key(key_value)
                    list_keys.append(n_key_value)
                return tuple(list_keys)
            if isinstance(cls_key, str):
                key_value = kwargs.get(cls_key)
                assert key_value is not None, f"{cls_key} not found!"
                return cls._normalize_key(key_value)
        except KeyError:
            raise Exception(f'{obj_name}, has no key defined!')


class MyID(metaclass=Meta):
    """
    This class uses the Meta metaclass to generate unique IDs for instances of the class.
    """
    __instances = {}
    __ids = {}
    __next_id = 1

    def __init__(self, *args, **kwargs):
        """
        This method assigns a unique ID to the instance and stores the key passed in the kwargs dictionary.
        """
        self.id = self.__generate_unique_id()
        self.__ids[self.__class__.__name__][self.id] = kwargs.get('_key', '_')

    @classmethod
    def __generate_unique_id(cls):
        """
        This method generates a unique ID for the instance and increments the next ID counter.
        """
        new_id = cls.__next_id
        cls.__next_id += 1
        return new_id

    @classmethod
    def __set_instance(cls, _key, instance):
        """
        This method stores the instance in the __instances dictionary using the key passed.
        """
        if cls.__name__ not in cls.__instances.keys():
            cls.__instances[cls.__name__] = {}
            cls.__ids[cls.__name__] = {}

        if _key:
            if _key not in cls.__instances[cls.__name__].keys():
                cls.__instances[cls.__name__][_key] = instance
            else:
                raise ValueError(f'{_key} Already Exists!')

    @classmethod
    def get_instance(cls, _key=None, _id: int = None):
        """
        This method is used to retrieve an instance from the __instances dictionary using either the key or the ID of the instance.
        """
        instance_key = cls._normalize_key(_key)
        instance_id = _id
        if cls.__name__ in cls.__instances.keys():

            if instance_id is not None and instance_key is None:
                instance_key = cls.__ids[cls.__name__][instance_id]

            if instance_key is None:
                return cls.__instances[cls.__name__]

            if instance_key in cls.__instances[cls.__name__].keys():
                return cls.__instances[cls.__name__][instance_key]

    @classmethod
    def __del_instance(cls, _key=None, _id: int = None):
        """
        This method is used to delete an instance from the __instances dictionary using either the key or the ID of the instance.
        """
        instance_key = cls._normalize_key(_key)
        instance_id = _id

        if instance_id is not None and instance_key is None:
            instance_key = cls.__ids[cls.__name__][instance_id]

        if instance_key is None:
            cls.__instances[cls.__name__] = {}
        else:
            del cls.__instances[cls.__name__][instance_key]

    def __new__(cls, *args, **kwargs):
        """
        This method overrides the default new method and is used to return existing instances of the class
        instead of creating new ones. It also allows for the option to override an existing instance with a new one.
        """
        key = kwargs.get('_key', '_')  # The key to identify the instance
        override = kwargs.get('_override', 1)  # Flag to indicate whether to override an existing instance or not
        instance = cls.get_instance(_key=key)  # Check if an instance with the same key already exists

        if instance and override == 1:
            cls.__del_instance(key)  # delete the existing instance if override is set to 1

        if instance is None or override == 1:
            # create a new instance of the class
            instance = super().__new__(cls)
            cls.__set_instance(key, instance)

        return instance


class Server(MyID):
    def __init__(self, server_name: str, **kwargs):
        super().__init__(**kwargs)
        self._server_name = server_name

    @property
    def server_name(self):
        return self._server_name


class DataSource(MyID):
    def __init__(self, source_name: str, source_level: int, scheduled: int, active: int = 1, **kwargs):
        super().__init__(**kwargs)
        self._source_name = source_name
        self.source_level = source_level
        self.scheduled = scheduled
        self.active = active

    @property
    def source_name(self):
        return self._source_name

    @property
    def tables(self):
        return [Table.get_instance(key) for key in Table.get_instance().keys() if self.source_name == Table.get_instance(key).source_name]


class Schema(MyID):
    def __init__(self, schema_name: str, is_tmp: int = 0, notes: str = None, **kwargs):
        super().__init__(**kwargs)
        self._schema_name = schema_name
        self.is_tmp = is_tmp
        self.notes = notes

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def tables(self):
        return [Table.get_instance(key) for key in Table.get_instance().keys() if self.schema_name == key[0]]


class DataType(MyID):
    def __init__(self, data_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_type = data_type


class Table(MyID):
    """
    key is: schema_id & table_name
    """

    def __init__(self, schema_id: int, table_name: str, table_kind: str, source_id: int = None, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_id = schema_id
        self._table_name = table_name
        self.source_id = source_id
        self._table_kind = table_kind
        self.active = active

    @property
    def table_kind(self):
        return self._table_kind.strip().upper()

    @property
    def table_name(self):
        return self._table_name.strip().lower()

    @property
    def schema(self):
        return Schema.get_instance(_key=None, _id=self.schema_id)


class DataSetType(MyID):
    def __init__(self, set_type: str, **kwargs):
        super().__init__(**kwargs)
        self.set_type = set_type


class DataSet(MyID):
    def __init__(self, set_type_id: int, set_code: str, set_name: str, table_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_type_id = set_type_id
        self.set_code = set_code
        self.set_name = set_name
        self.table_id = table_id

    @classmethod
    def get_bmaps(cls):
        set_type = DataSetType.get_instance(_key='BMAP')
        return [ds for ds in cls.get_instance().values() if ds.set_type_id == set_type.id]

    @classmethod
    def get_bkeys(cls):
        set_type = DataSetType.get_instance(_key='bkey')
        return [ds for ds in cls.get_instance().values() if ds.set_type_id == set_type.id]


class Domain(MyID):
    def __init__(self, data_set_id, domain_code, domain_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_set_id = data_set_id
        self.domain_code = domain_code
        self.domain_name = domain_name


class DomainValue(MyID):
    def __init__(self, domain_id, source_key, edw_key, description, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_id = domain_id
        self.source_key = source_key
        self.edw_key = edw_key
        self.description = description


class Column(MyID):
    def __init__(self, table_id: int, column_name: str, is_pk: int = 0, is_start_date: int = 0, is_end_date: int = 0, is_created_at: int = 0
                 , is_updated_at: int = 0, is_created_by: int = 0, is_updated_by: int = 0, is_delete_flag: int = 0, is_modification_type: int = 0
                 , is_load_id: int = 0, is_batch_id: int = 0, is_row_identity: int = 0, scd_type: int = 1, domain_id=None, data_type_id=None,
                 dt_precision=None, active=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_id = table_id
        self.column_name = column_name
        self.is_pk = is_pk
        self.is_start_date = is_start_date
        self.is_end_date = is_end_date
        self.is_created_at = is_created_at
        self.is_updated_at = is_updated_at
        self.is_created_by = is_created_by
        self.is_updated_by = is_updated_by
        self.is_delete_flag = is_delete_flag
        self.is_modification_type = is_modification_type
        self.is_load_id = is_load_id
        self.is_batch_id = is_batch_id
        self.is_row_identity = is_row_identity
        self.scd_type = scd_type
        self.domain_id = domain_id
        self.data_type_id = data_type_id
        self.dt_precision = dt_precision
        self.active = active


class SMX:
    SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
        , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping']

    @time_elapsed_decorator
    def __init__(self, path):
        self.path = path
        self.xls = pd.ExcelFile(path)
        self.data = {}
        Schema(schema_name='gdev1v_stg_online')
        Schema(schema_name='gdev1t_stg')
        Schema(schema_name='gdev1t_utlfw')
        Schema(schema_name='gdev1v_srci')
        DataSetType(set_type='bkey')
        DataSetType(set_type='BMAP')

    @time_elapsed_decorator
    def parse_file(self):
        for sheet in self.xls.sheet_names:
            self.parse_sheet(sheet)

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
        self.extract_staging_tables()
        self.extract_bkeys()
        self.extract_bmaps()
        self.extract_bmap_values()
        self.extract_data_types()
        self.extract_stg_columns()

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

        bmaps_lst = DataSet.get_bmaps()
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

    @time_elapsed_decorator
    def extract_staging_tables(self):
        def stg_tables(row):
            ds = DataSource.get_instance(_key=row.schema)
            if ds:
                Table(schema_id=src_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                Table(schema_id=stg_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                Table(schema_id=srci_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)

        src_schema = Schema.get_instance(_key='gdev1v_stg_online')
        stg_schema = Schema.get_instance(_key='gdev1t_stg')
        srci_schema = Schema.get_instance(_key='gdev1v_srci')

        self.data['stg_tables'][['schema', 'table_name']].drop_duplicates().apply(stg_tables, axis=1)

    @time_elapsed_decorator
    def extract_data_types(self):
        def data_types(row):
            data_type_lst = row.data_type.split(sep='(')
            DataType(data_type=data_type_lst[0])

        self.data['stg_tables'][['data_type']].drop_duplicates().apply(data_types, axis=1)

    @time_elapsed_decorator
    def extract_stg_columns(self):
        def stg_columns(row):
            stg_table = Table.get_instance(_key=(stg_schema.id, row.table_name))
            srci_table = Table.get_instance(_key=(srci_schema.id, row.table_name))

            if srci_table:
                Column(table_id=srci_table.id, column_name=row.column_name)

            if stg_table and row.natural_key != '':
                data_type_lst = row.data_type.split(sep='(')
                _data_type = data_type_lst[0]
                data_type = DataType.get_instance(_key=_data_type)
                if data_type:
                    pk = 1 if row.pk.upper() == 'Y' else 0
                    precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                    Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, data_type_id=data_type.id, dt_precision=precision)

        stg_schema = Schema.get_instance(_key='gdev1t_stg')
        srci_schema = Schema.get_instance(_key='gdev1v_srci')
        df_stg_tables = self.data['stg_tables'][['table_name', 'column_name', 'data_type', 'natural_key', 'pk']].drop_duplicates()
        df_stg_tables.apply(stg_columns, axis=1)

        # q = """select distinct table_name, column_name, data_type, pk , natural_key from df_stg_tables; """
        # df_stg_cols = sqldf(q, locals())
        # df_stg_cols.apply(stg_columns, axis=1)
