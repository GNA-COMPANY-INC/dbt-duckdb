from typing import Any
from typing import Dict

from deltalake import DeltaTable, write_deltalake, WriterProperties

from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig
from . import pd_utils
import traceback

# GNA-COMPANY custom plugin
class Plugin(BasePlugin):
    wp = WriterProperties(compression="ZSTD", compression_level=9)
    
    def initialize(self, plugin_config: Dict[str, Any]):
        self.plugin_config = plugin_config
        
    def __delta_conn_opt(self, region:str, storage:str)->Dict[str, str]:
        conn = self.plugin_config[region][storage]
        host = conn.get("host")
        port = conn.get("port")
        access_key = conn.get("access_key")
        secret_key = conn.get("secret_key")
        region_str = conn.get("s3_region", "")
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
    
    def load(self, source_config: SourceConfig):
        storage = source_config.get("storage")
        region = source_config.get("region")
        if storage is None or region is None:
            raise Exception("storage and region are required arguments")

        if storage == "minio":        
            storage_options = self.__delta_conn_opt(region, storage)
            if "delta_table_path" not in source_config:
                raise Exception("'delta_table_path' is a required argument for the delta table")

            table_path = source_config["delta_table_path"]
            
            if storage_options:
                dt = DeltaTable(table_path, storage_options=storage_options)
            else:
                dt = DeltaTable(table_path)

            df = dt.to_pyarrow_dataset()
            return df
        
        else:
            # recordbatch
            raise NotImplementedError("Only deltalake in minio is supported")

    def store(self, target_config: TargetConfig):
        
        region = target_config.config.get("region")
        storage = target_config.config.get("storage")
        if storage is None or region is None:
            raise Exception("storage and region are required arguments")
        
        if storage == "minio":
            storage_options = self.__delta_conn_opt(region, storage)
            bucket = target_config.config.get("bucket")
            dataset_name = target_config.config.get("dataset_name")
            partition_col = target_config.config.get("partition_col", None)
            if not bucket or not dataset_name:
                raise Exception(f"bucket: {bucket} and dataset_name: {dataset_name} are both required")
            df = pd_utils.target_to_df(target_config)
            try:
                write_deltalake(
                    f"s3://{bucket}/{dataset_name}",
                    data=df,
                    schema=None,
                    partition_by=partition_col,
                    name="test_dbt",
                    description="",
                    storage_options=storage_options,
                    mode="append",
                    writer_properties=self.wp,
                    engine="rust",
                    custom_metadata=None,
                )
                
            except Exception:
                raise Exception(f"Failed to save datasource {traceback.format_exc()}")        
        else:
            raise NotImplementedError("Only delta format is supported")
        
    def default_materialization(self):
        return "view"
