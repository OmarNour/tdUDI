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
        , 'Layer': 'layer_name'
        , 'LayerTable': ('layer_id', 'table_id')
        , 'Pipeline': ('src_lyr_table_id', 'tgt_lyr_table_id')
        , 'ColumnMapping': ('pipeline_id', 'col_seq', 'tgt_col_id')
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
                    assert key_value is not None, f"Key value cannot be None!"
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

            if instance_id in cls.__ids[cls.__name__].keys():
                instance_key = cls.__ids[cls.__name__][instance_id]

            if instance_key is None:
                return cls.__instances[cls.__name__].values()

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
    def tables(self) -> []:
        return [table for table in Table.get_instance() if self.id == table.data_source.id]


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
    def tables(self) -> []:
        return [table for table in Table.get_instance() if self.id == table.schema.id]


class DataType(MyID):
    def __init__(self, data_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_type = data_type


class Table(MyID):
    """
    key is: schema_id & table_name
    """

    def __init__(self, schema_id: int, table_name: str, table_kind: str
                 , source_id: int = None, multiset: int = 1, active: int = 1, ddl: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_id = schema_id
        self._table_name = table_name
        self.source_id = source_id
        self._table_kind = table_kind
        self.active = active
        self.multiset = multiset
        self._ddl = ddl

    @property
    def data_source(self) -> DataSource:
        return DataSource.get_instance(_id=self.source_id)

    @property
    def columns(self) -> []:
        col: Column
        return [col for col in Column.get_instance() if col.table_id == self.id]

    @property
    def table_kind(self):
        return self._table_kind.strip().upper()

    @property
    def table_name(self):
        return self._table_name.strip().lower()

    @property
    def schema(self) -> Schema:
        return Schema.get_instance(_id=self.schema_id)

    @property
    def ddl(self) -> str:
        if self.table_kind == 'T':
            col_dtype = ''
            set_multiset = 'multiset' if self.multiset == 1 else 'set'
            schema_name = self.schema.schema_name
            table_name = self.table_name
            pi_cols_lst = []
            col: Column
            for idx, col in enumerate(self.columns, start=1):
                comma = ',' if idx > 1 else ''
                col_name = col.column_name
                data_type = col.data_type.data_type
                precision = "(" + str(col.dt_precision) + ")" if col.dt_precision else ''
                latin_unicode = "unicode" if col.unicode == 1 else "latin"
                not_case_sensitive = "not" if col.case_sensitive == 0 else ''
                not_null = 'not null' if col.mandatory == 1 else ''
                col_dtype = col_dtype + COL_DTYPE_TEMPLATE.format(col_name=col_name
                                                                  , data_type=data_type, precision=precision, latin_unicode=latin_unicode
                                                                  , not_case_sensitive=not_case_sensitive
                                                                  , not_null=not_null
                                                                  , comma=comma)
                pi_cols_lst.append(col.column_name) if col.is_pk else None

            pi_index = PI_TEMPLATE.format(pi_cols=list_to_string(list=pi_cols_lst, separator=',')) if len(pi_cols_lst) > 0 else 'No Primary Index'
            self._ddl = DDL_TABLE_TEMPLATE.format(set_multiset=set_multiset
                                                  , schema_name=schema_name
                                                  , table_name=table_name
                                                  , col_dtype=col_dtype
                                                  , pi_index=pi_index
                                                  , si_index='')

        return self._ddl


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
    def get_bmaps(cls) -> []:
        set_type = DataSetType.get_instance(_key='BMAP')
        return [ds for ds in cls.get_instance() if ds.set_type_id == set_type.id]

    @classmethod
    def get_bkeys(cls) -> []:
        set_type = DataSetType.get_instance(_key='bkey')
        return [ds for ds in cls.get_instance() if ds.set_type_id == set_type.id]

    @property
    def data_set_type(self) -> DataSetType:
        return DataSetType.get_instance(_id=self.set_type_id)


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
    def __init__(self, table_id: int, column_name: str, is_pk: int = 0, mandatory: int = 0, is_start_date: int = 0, is_end_date: int = 0, is_created_at: int = 0
                 , is_updated_at: int = 0, is_created_by: int = 0, is_updated_by: int = 0, is_delete_flag: int = 0, is_modification_type: int = 0
                 , is_load_id: int = 0, is_batch_id: int = 0, is_row_identity: int = 0, scd_type: int = 1, domain_id=None, data_type_id=None,
                 dt_precision: int = None, unicode: int = 0, case_sensitive: int = 0, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_id = table_id
        self.column_name = column_name
        self.is_pk = is_pk
        self.mandatory = mandatory
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
        self.unicode = unicode
        self.case_sensitive = case_sensitive

    @property
    def data_type(self) -> DataType:
        return DataType.get_instance(_id=self.data_type_id)

    @property
    def table(self) -> Table:
        return Table.get_instance(_id=self.table_id)


class Layer(MyID):
    def __init__(self, layer_name, abbrev=None, layer_level=None, active=1, notes=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert layer_level is not None
        self.layer_name = layer_name
        self.abbrev = abbrev
        self.layer_level = layer_level
        self.active = active
        self.notes = notes

    @property
    def tables(self) -> []:
        return [lt.table for lt in LayerTable.get_instance() if lt.layer_id == self.id]


class LayerTable(MyID):
    def __init__(self, layer_id: int, table_id: int, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.layer_id = layer_id
        self.table_id = table_id
        self.active = active

    @property
    def table(self) -> Table:
        return Table.get_instance(_id=self.table_id)

    @property
    def layer(self) -> Layer:
        return Layer.get_instance(_id=self.layer_id)


class Pipeline(MyID):
    def __init__(self, src_lyr_table_id: int, tgt_lyr_table_id: int, schema_id: int, active: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src_lyr_table_id = src_lyr_table_id
        self.tgt_lyr_table_id = tgt_lyr_table_id
        self.schema_id = schema_id
        self.active = active

    @property
    def schema(self) -> Schema:
        return Schema.get_instance(_id=self.schema_id)

    @property
    def src_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self.src_lyr_table_id)

    @property
    def tgt_lyr_table(self) -> LayerTable:
        return LayerTable.get_instance(_id=self.tgt_lyr_table_id)


class ColumnMapping(MyID):
    def __init__(self, pipeline_id, tgt_col_id, col_seq: int = 0, src_col_id=None, src_col_trx=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline_id = pipeline_id
        self.col_seq = col_seq
        self.src_col_id = src_col_id
        self.tgt_col_id = tgt_col_id
        self.src_col_trx = src_col_trx

    @property
    def pipeline(self) -> Pipeline:
        return Pipeline.get_instance(_id=self.pipeline_id)

    @property
    def tgt_col(self) -> Column:
        return Column.get_instance(_id=self.tgt_col_id)

    @property
    def src_col(self) -> Column:
        return Column.get_instance(_id=self.src_col_id)

    @property
    def valid_src_col_trx(self) -> bool:
        return True

    @property
    def ddl(self):
        return ''
