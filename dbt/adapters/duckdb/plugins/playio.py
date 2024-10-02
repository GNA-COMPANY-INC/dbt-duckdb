from typing import Any
from typing import Dict

from deltalake import DeltaTable, write_deltalake, WriterProperties
from duckdb import DuckDBPyConnection
from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig
from . import pd_utils
import traceback
import os
import pandas
import datetime
from typing import List
# ======== GNA-COMPANY custom plugin for dbt-duckdb ========
# - made to supports multi-source read and multi-target write

# ==== changelog ====
# - 2024-08-20 
#   - init fork: https://github.com/GNA-COMPANY-INC/dbt-duckdb
#   - support deltalake table from minio: both read and write

# - 2024-09-22 update
#   - support mysql table: read and write
#   - support local file: write
#   - tested dbt-python-model functionality

# - 2024-09-24 update
#   - create table if not exists for rds
#   - support unique_cols and index_cols for rds

# - 2024-10-02 update
#   - block read from rds due to performance issue. use query python model instead
#   - block auto generate table for rds. print ddl instead

class Plugin(BasePlugin):
    wp = WriterProperties(compression="ZSTD", compression_level=9)
    
    __rds_default_cols = [
        "id BIGINT PRIMARY KEY AUTO_INCREMENT not null",
        "created TIMESTAMP DEFAULT CURRENT_TIMESTAMP null",
        "updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP null",
    ]
    
    __rds_default_dtypes = {
        'int32': 'BIGINT',
        'int64': 'BIGINT',
        'float32': 'DOUBLE',
        'float64': 'DOUBLE',
        'object': 'VARCHAR(512)',
        'datetime64[ns]': 'DATETIME',
        'datetime64[ns, UTC]': 'DATETIME',
        'timedelta64[ns]': 'TIME',
        'bool': 'BOOLEAN',
        'datetime.date': 'DATE',
        'datetime.datetime': 'DATETIME'
    }
    
    
    
    def initialize(self, plugin_config: Dict[str, Any])->None:
        self.plugin_config = plugin_config
            
    def configure_connection(self, conn: DuckDBPyConnection)->None:
        self.conn = conn        
        
        def current_datetime_local(fmt:str, days:int=0)->str:
            return (datetime.datetime.now() + datetime.timedelta(days=days)).strftime(fmt)
        
        self.conn.create_function(name="current_datetime_local", function=current_datetime_local)
        
    def __delta_conn_opt(self, region:str, storage:str)->Dict[str, str]:
        __opt = self.plugin_config[region][storage]
        
        host = __opt.get("host")
        port = __opt.get("port")
        access_key = __opt.get("access_key")
        secret_key = __opt.get("secret_key")
        region_str = __opt.get("s3_region", "")
        return {
            "aws_endpoint": f"http://{host}:{port}",
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "scheme": "http",
            "aws_region": region_str,
            "AWS_ALLOW_HTTP": "True",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
            "AWS_STORAGE_ALLOW_HTTP": "True",
        }
        
    def __rds_conn_opt(self, region:str, storage:str)->Dict[str, str]:
        __opt = self.plugin_config[region][storage]
        
        host = __opt.get("host")
        port = __opt.get("port")
        user = __opt.get("user")
        password = __opt.get("password")
        
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }

    def __attach_to_rds(self, storage_options:Dict, scheme:str, table_name:str)->str:
        h = storage_options.get("host")
        u = storage_options.get("user")
        p = storage_options.get("port")
        pwd = storage_options.get("password")
        db_alias = f"{scheme}_{table_name}"     
        self.conn.execute(
            f"ATTACH 'host={h} user={u} port={p} password={pwd} database={scheme}' AS {db_alias} (TYPE MYSQL);"
        )
        self.conn.execute(f"USE {db_alias};")   
        print(f"connected to rds: {table_name}.{scheme}")
        return db_alias
    
    
    def __create_table_if_needed(self, scheme:str, table_name:str, db_alias:str, df:pandas.DataFrame, uniq_cols:List[str]|str=None, index_cols:List[str]|str=None)->str:
        
        info_q = f"""
                select TABLE_NAME 
                from INFORMATION_SCHEMA.TABLES 
                where TABLE_SCHEMA = ''{scheme}'' and TABLE_NAME = ''{table_name}'';            
        """
        info_q = f"""
            select * from mysql_query('{db_alias}', '{info_q}');
        """
        
        if len(self.conn.execute(info_q).fetchall()) == 0:
            print(f"table {scheme}.{table_name} does not exist, creating...")
            ddl_cols = self.__rds_default_cols
            for column, dtype in zip(df.columns, df.dtypes):
                if pandas.api.types.is_datetime64_any_dtype(dtype):
                    mysql_type = self.__rds_default_dtypes['datetime64[ns]']
                elif isinstance(df[column].iloc[0], datetime.date):
                    mysql_type = self.__rds_default_dtypes['datetime.date']
                elif isinstance(df[column].iloc[0], datetime.datetime):
                    mysql_type = self.__rds_default_dtypes['datetime.datetime']
                elif pandas.api.types.is_integer_dtype(dtype):
                    mysql_type = self.__rds_default_dtypes['int64']
                elif pandas.api.types.is_float_dtype(dtype):
                    mysql_type = self.__rds_default_dtypes['float64']
                else:
                    max_data_len = df[column].apply(lambda x: len(str(x))).max() * 2
                    
                    mysql_type = f'VARCHAR({str(max_data_len)})'

                _col_ddl = f"  {column} {mysql_type}"
                ddl_cols.append(_col_ddl)
            
            
            if uniq_cols is not None:
                if isinstance(uniq_cols, str):
                    uniq_cols = [uniq_cols]
                _unq_ddl = f"constraint uq_{table_name}_{'_'.join(uniq_cols)} unique ({', '.join(uniq_cols)})"
                ddl_cols.append(_unq_ddl)
                
            ddl_cols_str = ",\n".join(ddl_cols)
            create_q = f"""
CREATE TABLE {scheme}.{table_name} (
    {ddl_cols_str}
); 
"""
            print(f"create ddl: {create_q}")
            # create_q = f"""
            #     CALL mysql_execute('{db_alias}', '{create_q}');
            # """
            # self.conn.execute(create_q)
            
            index_ddl = [
                f"create index idx_created on {scheme}.{table_name} (created);",
                f"create index idx_updated on {scheme}.{table_name} (updated);",   
            ]
            
            if index_cols is not None:
                if isinstance(index_cols, str):
                    index_cols = [index_cols]
                for idx in index_cols:
                    _ddl = f"create index idx_{idx} on {scheme}.{table_name} ({idx});"
                    index_ddl.append(_ddl)
                
            index_q = "\n".join(index_ddl)
            # index_q = f"""
            #     CALL mysql_execute('{db_alias}', '{index_q}'); 
            # """
            # self.conn.execute(index_q)
            print(f"created table: {scheme}.{table_name}")
            
            ddl = f"{create_q}{index_q}"
            return ddl
        else:
            print(f"table {scheme}.{table_name} already exists")
            return None
                    
        
    def load(self, source_config: SourceConfig)->Any:
        print(f"loading source_config: {source_config}")
        storage = source_config.get("storage")
        region = source_config.get("region")
        if storage is None or region is None:
            raise Exception("storage and region are required arguments")
        storage_type = self.plugin_config[region][storage].get("type")
        
        if storage_type == "delta":        
            storage_options = self.__delta_conn_opt(region, storage)
            if "delta_table_path" not in source_config:
                raise Exception("'delta_table_path' is a required argument for the delta table")
            table_path = source_config["delta_table_path"]
            print(f"loading from minio deltalake table: {region}.{storage}.{table_path}")
            if storage_options:
                dt = DeltaTable(table_path, storage_options=storage_options)
            else:
                dt = DeltaTable(table_path)

            df = dt.to_pyarrow_dataset()
            return df
        
        elif storage_type == "rds":
            raise NotImplementedError("Only deltalake in minio is supported")
            table_name = source_config.get("table")
            scheme = source_config.get("scheme")
            if table_name is None or scheme is None:
                raise Exception("table and scheme are required arguments for rds")
            storage_options = self.__rds_conn_opt(region, storage)
            self.__attach_to_rds(storage_options, scheme, table_name)
            print(f"loading from rds table: {region}.{storage}.{table_name}")
            rel =  self.conn.table(table_name=table_name).fetch_arrow_reader()
            return rel
            
        else:
            raise NotImplementedError("Only deltalake in minio is supported")


    def store(self, target_config: TargetConfig)->None:
        
        region = target_config.config.get("region")
        storage = target_config.config.get("storage")
        storage_type = self.plugin_config[region][storage].get("type")
        if storage is None or region is None:
            raise Exception("storage and region are required arguments")
        
        if storage_type == "delta":        
            storage_options = self.__delta_conn_opt(region, storage)
            print(f"store storage_options: {storage_options}")
            bucket = target_config.config.get("bucket")
            dataset_name = target_config.config.get("dataset_name")
            partition_col = target_config.config.get("partition_col", None)
            if not bucket or not dataset_name:
                raise Exception(f"bucket: {bucket} and dataset_name: {dataset_name} are both required")
            print(f"storing to minio: {region}.{storage} s3://{bucket}/{dataset_name}")
            df = pd_utils.target_to_df(target_config)
            print(f"df:\n{df}")
            try:
                write_deltalake(
                    f"s3://{bucket}/{dataset_name}",
                    data=df,
                    schema=None,
                    partition_by=partition_col,
                    name=dataset_name,
                    description="",
                    storage_options=storage_options,
                    mode="append",
                    writer_properties=self.wp,
                    engine="rust",
                    custom_metadata=None,
                )
                print(f"saved to minio: {region}.{storage} s3://{bucket}/{dataset_name}")
            except Exception:
                msg = f"Failed to save datasource {traceback.format_exc()}"
                print(msg)
                raise Exception(msg)
            
        elif storage_type == "rds":
            scheme = target_config.config.get("scheme")
            table_name = target_config.config.get("table")
            if table_name is None or scheme is None:
                raise Exception("table and scheme are required arguments for rds")
            storage_options = self.__rds_conn_opt(region, storage)
            db_alias = self.__attach_to_rds(storage_options, scheme, table_name)
            
            print(f"storing to rds table: {region}.{storage} {scheme}.{table_name}")
            
            df = pd_utils.target_to_df(target_config)
            print(f"df:\n{df}")
            
            uniq_cols = target_config.config.get("unique_cols", None)
            index_cols = target_config.config.get("index_cols", None)
            ddl = self.__create_table_if_needed(scheme, table_name, db_alias, df, uniq_cols, index_cols)
            if ddl is not None:
                print(f"CREATE TABLE FIRST IN {region} {storage}:\n========== DDL ==========\n{ddl}\n=========================")
                return
            
            cols = df.columns
            col_str = ", ".join(cols)
            
            vals = []
            for _,row in df.iterrows():
                vals_str = ", ".join([f"NULL" if v is None else f"''{v}''" for v in row])
                vals.append(f"({vals_str})")
            
            q = f"""
                insert into
                {scheme}.{table_name}
                ({col_str})
                values
                {", ".join(vals)}
                on duplicate key update
                {", ".join([f"{c}=values({c})" for c in cols])}
            """
                        
            q = f"""
            CALL mysql_execute('{db_alias}', '{q}');
            """
            
            self.conn.execute(q)
            print(f"inserted to rds table: {scheme}.{table_name}, row count: {len(df)}")
            
        elif storage_type == "local":
            df = pd_utils.target_to_df(target_config)
            print(f"df:\n{df}")
            root = self.plugin_config[region][storage].get("root")
            path = target_config.config.get("file_name")
            path = os.path.join(root, path)
            if path.endswith(".csv"):
                if os.path.exists(path):
                    os.remove(path)
                df.to_csv(path, index=False)
            elif path.endswith(".parquet"):
                if os.path.exists(path):
                    os.remove(path)
                df.to_parquet(path, index=False)
            else:
                raise Exception(f"Unsupported format: {path}")
            print(f"saved to local path: {path}")
            
        elif storage_type == "ref":
            df = pd_utils.target_to_df(target_config)
            print(f"df:\n{df}")
            pass 
        
        else:
            raise NotImplementedError("unsupported storage type")
        
    def default_materialization(self):
        return "view"
