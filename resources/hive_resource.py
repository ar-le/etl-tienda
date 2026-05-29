from dagster import ConfigurableResource
from sqlalchemy import create_engine, Engine
from hdfs import InsecureClient

class HiveResource(ConfigurableResource):
    """Resource para conectarse a Hive (SQL) y HDFS (Archivos)"""
    
    hive_ip: str
    username: str
    database: str
    hive_port: int = 10000    # Puerto de HiveServer2 (Thrift)
    hdfs_port: int = 9870     # Puerto de WebHDFS (Hadoop 3.x)

    def get_engine(self) -> Engine:
        """Crea y retorna un SQLAlchemy Engine para la conexión a Hive"""
        connection_string = f"hive://{self.username}@{self.hive_ip}:{self.hive_port}/{self.database}"
        return create_engine(connection_string)

    def get_hdfs_client(self) -> InsecureClient:
        """Crea y retorna un cliente para interactuar con WebHDFS"""
        # Formato: http://<IP>:<PORT>
        hdfs_url = f"http://{self.hive_ip}:{self.hdfs_port}"
        return InsecureClient(hdfs_url, user=self.username, timeout=60)