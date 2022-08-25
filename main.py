from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, DataFormat, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, ReportLevel, ReportMethod
import pyodbc
import pandas as pd
import setting

### config
KUSTO_TABLE = "sql_dbo_AllocationHistory"

df = pd.read_sql_query("SELECT * FROM [dbo].[UserTransaction]", setting.conn)

### RUN INGEST

INGESTION_CLIENT = QueuedIngestClient(setting.KCSB_INGEST)

INGESTION_PROPERTIES = IngestionProperties(database=setting.KUSTO_DATABASE, table=KUSTO_TABLE, data_format=DataFormat.CSV)
            
INGESTION_CLIENT.ingest_from_dataframe(df, INGESTION_PROPERTIES)


##----create table------
# KUSTO_CLIENT = KustoClient(KCSB_DATA)
# CREATE_TABLE_COMMAND = ".create table StormEvents (StartTime: string, EndTime: string)"
# RESPONSE = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, CREATE_TABLE_COMMAND)

##----sent query to adx----

# QUERY = "Coupon | count"
# KUSTO_CLIENT = KustoClient(KCSB_DATA)
# RESPONSE = KUSTO_CLIENT.execute_query(KUSTO_DATABASE, QUERY)

# print(dataframe_from_result_table(RESPONSE.primary_results[0]))