import azure.functions as func
import pandas as pd
import logging
import datetime
import json
import uuid
import os
from azure.cosmos import CosmosClient
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties

app = func.FunctionApp()

# 環境変数の取得
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME")
ADX_CLUSTER = os.environ.get("ADX_CLUSTER")
ADX_DATABASE = os.environ.get("ADX_DATABASE")
ADX_TABLE = os.environ.get("ADX_TABLE")
KUSTO_CLIENT_ID = os.environ.get("KUSTO_CLIENT_ID")
KUSTO_CLIENT_SECRET = os.environ.get("KUSTO_CLIDNT_SECRET")
AUTHORITY_ID = os.environ.get("AUTHORITY_ID")

# 接続設定 (Kusto クライアントの設定)
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    ADX_CLUSTER, KUSTO_CLIENT_ID, KUSTO_CLIENT_SECRET, AUTHORITY_ID
)
kusto_client = KustoClient(kcsb)
queued_ingest_client = QueuedIngestClient(kcsb)

@app.function_name(name="eventhub_trigger")
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="MyEventHub",
    connection="EventHubConnectionAppSetting",
    consumer_group="coldpath"
) 
def eventhub_trigger(azeventhub: func.EventHubEvent):
    event_message = azeventhub.get_body().decode('utf-8')
    logging.info(
        'Python EventHub trigger processed an event: %s',
        event_message
    )
    save_db(event_message)


@app.timer_trigger(
    schedule="* */5 * * * *",
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=False
) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')


@app.cosmos_db_trigger(
    arg_name="azcosmosdb",
    container_name="aoai-container",
    database_name="iot-database",
    connection="COSMOS_CONNECTION_STRING",
    lease_container_name="lease-container",
    lease_database_name="iot-database"
) 
def cosmosdb_trigger(azcosmosdb: func.DocumentList):
    logging.info('Python CosmosDB triggered.')
    try:
        # 各ドキュメントに対する処理を実装
        for doc in azcosmosdb:
            # Document オブジェクトから JSON ドキュメントを取得
            json_document = doc.to_json()

            # JSON ドキュメントを Python 辞書にパース
            document_dict = json.loads(json_document)

            # 必要なキーのみ取得
            filtered_dict = {
                "Id": document_dict.get("Id"),
                "AnalysisResult": document_dict.get("AnalysisResult")
            }

            # Timestamp を追加
            dt_now_jst_aware = datetime.datetime.now(
                datetime.timezone(datetime.timedelta(hours=9))
            )
            filtered_dict["Timestamp"] = str(dt_now_jst_aware)

            # json を DataFrame に変換
            df_data = pd.DataFrame([filtered_dict])

            # Ingestion Propertiesの設定
            ingestion_props = IngestionProperties(
                database=ADX_DATABASE,
                table=ADX_TABLE
            )

            # データのインジェスト
            queued_ingest_client.ingest_from_dataframe(df_data, ingestion_properties=ingestion_props)

    except Exception as e:
        logging.error(f'Error processing document: {e}')


def save_db(event_message: str) -> None:
    logging.info('called save_db function.')
    client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    db = client.get_database_client(COSMOS_DATABASE_NAME)
    container = db.get_container_client(COSMOS_CONTAINER_NAME)
    dt_now_jst_aware = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9))
    )
    value = {
        "id": str(uuid.uuid4()),
        "partition": "1",
        "event_message": json.loads(event_message),
        "created_at": str(dt_now_jst_aware),
    }
    container.create_item(value)
