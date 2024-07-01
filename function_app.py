import azure.functions as func
import pandas as pd
import logging
import json
import uuid
import os
from io import BytesIO
from typing import Any, Dict, List
from openai import AzureOpenAI
import matplotlib.pyplot as plt
from azure.cosmos import CosmosClient, ContainerProxy
from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
from azure.core.paging import ItemPaged
from datetime import datetime, timezone, timedelta

app = func.FunctionApp()

# 環境変数の取得
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME")
COSMOS_AOAI_CONTAINER_NAME = os.environ.get("COSMOS_AOAI_CONTAINER_NAME")
COSMOS_IOT_HUB_CONTAINER_NAME = os.environ.get("COSMOS_IOT_HUB_CONTAINER_NAME")
BLOB_STORAGE_ACCOUNT = os.environ.get("BLOB_STORAGE_ACCOUNT")
BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME")
ADX_CLUSTER = os.environ.get("ADX_CLUSTER")
ADX_DATABASE = os.environ.get("ADX_DATABASE")
ADX_TABLE = os.environ.get("ADX_TABLE")
KUSTO_CLIENT_ID = os.environ.get("KUSTO_CLIENT_ID")
KUSTO_CLIENT_SECRET = os.environ.get("KUSTO_CLIENT_SECRET")
AUTHORITY_ID = os.environ.get("AUTHORITY_ID")
MESSAGE_TYPE = os.environ.get("MESSAGE_TYPE")
AZURE_OPENAI_SERVICE = os.environ.get("AZURE_OPENAI_SERVICE")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_OPENAI_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_TOKEN = os.environ.get("AZURE_OPENAI_TOKEN")

# Kusto クライアントの設定
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    ADX_CLUSTER, KUSTO_CLIENT_ID, KUSTO_CLIENT_SECRET, AUTHORITY_ID
)
kusto_client = KustoClient(kcsb)
queued_ingest_client = QueuedIngestClient(kcsb)

# Ingestion Propertiesの設定
ingestion_props = IngestionProperties(
    database=ADX_DATABASE,
    table=ADX_TABLE
)

# AOAI クライアントの設定
openai_client = AzureOpenAI(
    azure_endpoint = f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com",
    api_version=AZURE_OPENAI_API_VERSION,
    api_key = AZURE_OPENAI_TOKEN
)

# システムプロンプトの設定
system_prompt = """
あなたはセンサーデータの時間変化のグラフから、センサーの異常検知を分析する専門家です。
以下の情報と回答方法をもとに、回答を生成してください。

## グラフの情報
・青色はセンサー１、橙色はセンサー２，緑色はセンサー３のデータを表しています。
・正常状態では繰り返しの波形になり、異常状態では波形の最大値や最小値が常に変化した波形になります。
・明確に最大値や最小値が変化しない場合は、正常状態とみなしてください。

## 回答方法
・各センサーに対して、正常状態の場合は「センサーの値は正常です」と回答し、異常状態の場合は「センサーが異常を検出しました」と回答して下さい。
・また、各センサーデータのグラフの波形から読み取って分析できることについて、３０文字程度にまとめて回答して下さい。
・回答形式は、以下の json のフォーマットに従って下さい。
{"sensor1_status":"", "sensor2_status":"", "sensor3_status":"", "sensor1_analysis": "", "sensor2_analysis": "", "sensor3_analysis": ""}
"""

# Cosmos DB クライアントの設定
cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
database_client = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
aoai_container_client = database_client.get_container_client(COSMOS_AOAI_CONTAINER_NAME)
iot_hub_container_client = database_client.get_container_client(COSMOS_IOT_HUB_CONTAINER_NAME)

# Blob Storage クライアントの設定
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
container_url = BLOB_STORAGE_ACCOUNT + "/" + BLOB_CONTAINER_NAME

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
    try:
        save_cosmos_db(event_message, iot_hub_container_client)
    except Exception as e:
        logging.error(f'Error in eventhub_trigger: {e}')


@app.timer_trigger(
    schedule="*/10 * * * * *",
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=False
) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function executed.')
    try:
        # blob 名の設定
        current_time = get_current_time()
        blob_name = current_time.strftime("%Y-%m-%d %H:%M:%S") + '.png'

        # Blob Storage に保存される URL
        image_path = container_url + "/" + blob_name

        # Cosmos DB から json データの取得
        items = read_cosmos_container(iot_hub_container_client)

        # 取得した json データからセンサーデータの抽出
        sensor_data = extract_sensor_data(items)

        # センサーデータが含まれている場合
        if len(sensor_data) > 0:

            # センサーデータを昇順にソート
            sorted_sensor_data = sorted(sensor_data, key=lambda x: x['created_at'])

            # センサーデータから画像ファイルの出力
            img_buffer = create_graph_image(sorted_sensor_data)

            # 画像ファイルを保存
            save_graph_image_to_blob(img_buffer, blob_name)

            # AOAI を使った画像分析
            analysis_result = analyze_graph_with_aoai(image_path)

            # Id と PublisherId の追加
            analysis_result['Id'] = sensor_data[0].get('Id', '')
            analysis_result['PublisherId'] = sensor_data[0].get('PublisherId', '')
            analysis_result_str = json.dumps(analysis_result)

            # Cosmos DB への保存
            save_cosmos_db(analysis_result_str, aoai_container_client)
    except Exception as e:
        logging.error(f'Error in timer_trigger: {e}')


@app.cosmos_db_trigger(
    arg_name="azcosmosdb",
    container_name="aoai-container",
    database_name="iot-database",
    connection="COSMOS_CONNECTION_STRING",
    lease_container_name="aoai-lease-container",
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

            # メッセージを取得
            event_message = document_dict.get("event_message")

            # 必要なキーのみ取得
            filtered_dict = {
                "Id": event_message.get("Id"),
                "PublisherId": event_message.get("PublisherId"),
                "Sensor1Status": event_message.get("sensor1_status"),
                "Sensor2Status": event_message.get("sensor2_status"),
                "Sensor3Status": event_message.get("sensor3_status"),
                "Sensor1Analysis": event_message.get("sensor1_analysis"),
                "Sensor2Analysis": event_message.get("sensor2_analysis"),
                "Sensor3Analysis": event_message.get("sensor3_analysis"),
            }

            # Timestamp を追加
            dt_now_jst_aware = datetime.now(
                timezone(timedelta(hours=9))
            )
            filtered_dict["Timestamp"] = str(dt_now_jst_aware)

            # json を DataFrame に変換
            df_data = pd.DataFrame([filtered_dict])

            # データのインジェスト
            queued_ingest_client.ingest_from_dataframe(df_data, ingestion_properties=ingestion_props)

    except Exception as e:
        logging.error(f'Error in cosmosdb_trigger: {e}')


def save_cosmos_db(event_message: str, container_client: ContainerProxy) -> None:
    logging.info('called save_cosmos_db function.')
    current_time = get_current_time()
    value = {
        "id": str(uuid.uuid4()),
        "partition": "1",
        "event_message": json.loads(event_message),
        "created_at": str(current_time),
    }
    container_client.create_item(value)


def read_cosmos_container(container_client: ContainerProxy) -> ItemPaged[Dict[str, Any]]:
    # 現在のUTC時刻を取得し、30分前の時刻を計算
    current_time = get_current_time()
    time_5_minutes_ago = current_time - timedelta(minutes=5)
    time_5_minutes_ago_str = time_5_minutes_ago.isoformat()

    # クエリの作成
    query = f"""
    SELECT TOP 30 * 
    FROM c 
    WHERE c.event_message.MessageType = '{MESSAGE_TYPE}' 
    AND c.created_at >= '{time_5_minutes_ago}' 
    ORDER BY c.created_at DESC
    """

    # パラメーターの設定
    params = [
        {"name": "@time_5_minutes_ago", "value": time_5_minutes_ago_str}
    ]

    # クエリを実行し、結果を取得
    items = container_client.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    )
    return items


def extract_sensor_data(items: ItemPaged[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sensor_data = []
    for item in items:
        mapped_item = {
            "Id": item.get('id', ''),
            "PublisherId": item['event_message'].get('PublisherId', ''),
            "SignalColor": item['event_message']['Messages'][0]['Payload'].get('i=7', {}).get('Value', ''),
            "SignalMode": item['event_message']['Messages'][0]['Payload'].get('i=8', {}).get('Value', ''),
            "Sensor1RawValue": item['event_message']['Messages'][0]['Payload'].get('i=10', {}).get('Value', ''),
            "Sensor2RawValue": item['event_message']['Messages'][0]['Payload'].get('i=12', {}).get('Value', ''),
            "Sensor3RawValue": item['event_message']['Messages'][0]['Payload'].get('i=14', {}).get('Value', ''),
            "State": item['event_message']['Messages'][0]['Payload'].get('i=15', {}).get('Value', ''),
            "created_at": item.get('created_at', '')
        }
        sensor_data.append(mapped_item)
    return sensor_data


def create_graph_image(sensor_data: List[Dict[str, Any]]) -> BytesIO:
    # データを抽出
    timestamps = [data['created_at'] for data in sensor_data]
    sensor1_values = [data['Sensor1RawValue'] for data in sensor_data]
    sensor2_values = [data['Sensor2RawValue'] for data in sensor_data]
    sensor3_values = [data['Sensor3RawValue'] for data in sensor_data]

    # グラフの作成
    plt.figure(figsize=(10, 6))
    plt.plot(timestamps, sensor1_values, label='Sensor1RawValue')
    plt.plot(timestamps, sensor2_values, label='Sensor2RawValue')
    plt.plot(timestamps, sensor3_values, label='Sensor3RawValue')
    plt.xlabel('Timestamp')
    plt.ylabel('Value')
    plt.title('Sensor Data')
    plt.legend()

    # 横軸のタイムスタンプを非表示にする  
    plt.gca().axes.get_xaxis().set_visible(False)  

    # グラフをバイナリストリームに保存
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)

    # バッファを閉じる
    plt.close()

    return img_buffer


def save_graph_image_to_blob(img_buffer: BytesIO, blob_name: str) -> None:
    # Blob にアップロード
    blob_container_client.upload_blob(name=blob_name, data=img_buffer, blob_type="BlockBlob")

    # バッファを閉じる
    img_buffer.close()


def analyze_graph_with_aoai(image_path: str) -> dict:
    messages = [
        {"role":"system","content": system_prompt},
        {"role":"user","content":[
            {
                "type": "image_url",
                "image_url": {
                    "url": image_path
                }
            }
        ]}
    ]

    response = openai_client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=messages,
        temperature=0.0,
        max_tokens=1000,
        n=1
    )

    res = response.choices[0].message.content
    res_dict = json.loads(res)
    return res_dict


def get_current_time() -> datetime:
    current_time = datetime.now(
        timezone(timedelta(hours=9))
    )
    return current_time
