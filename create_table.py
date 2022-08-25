from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, DataFormat, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, ReportLevel, ReportMethod
import pandas as pd
import setting

QUERY = "evaluate sql_request('Server=tcp:utop-operation-server-pro.database.windows.net,1433;Initial Catalog=utop-operation-db-pro;Persist Security Info=False;User ID=hanhtv13;Password=1LU5GXLqh9zH', 'SELECT TOP 10 * FROM [dbo].[W_USER_D]') | getschema | project ColumnName, ColumnType"
KUSTO_CLIENT = KustoClient(setting.KCSB_DATA)
RESPONSE = KUSTO_CLIENT.execute_query(setting.KUSTO_DATABASE, QUERY)
TABLE_NAME = ""  ## TODO: fill name table

respone = dataframe_from_result_table(RESPONSE.primary_results[0])
a = ""
for i in range(respone.shape[0]):
    a = a + str(respone['ColumnName'][i]) + ': ' + str(respone['ColumnType'][i]) + ','
    a = a[ : -1]

KUSTO_CLIENT = KustoClient(setting.KCSB_DATA)
CREATE_TABLE_COMMAND = ".create table" + str(TABLE_NAME) + "(" + str(a) + ")"
RESPONSE = KUSTO_CLIENT.execute_mgmt(setting.KUSTO_DATABASE, CREATE_TABLE_COMMAND)


