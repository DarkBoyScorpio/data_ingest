from azure.kusto.data import KustoConnectionStringBuilder
import pyodbc

## KUSTO CONFIG
AAD_TENANT_ID = "f01e930a-b52e-42b1-b70f-a8882b5d043b"
KUSTO_URI = "https://utopadx.southeastasia.kusto.windows.net/"
KUSTO_INGEST_URI = "https://ingest-utopadx.southeastasia.kusto.windows.net/"
KUSTO_DATABASE = "utopAnalytics"

## SQL SERVER KUSTO CONFIG
server = 'tcp:utop-operation-server-pro.database.windows.net'
database = 'utop-operation-db-pro'
username = 'hanhtv13'
password = '1LU5GXLqh9zH'

## KUSTO CONNECT
KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(
    KUSTO_INGEST_URI, AAD_TENANT_ID)

KCSB_DATA = KustoConnectionStringBuilder.with_aad_device_authentication(
    KUSTO_URI, AAD_TENANT_ID)

## SQL SERVER CONNECT
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)