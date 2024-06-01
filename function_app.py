import azure.functions as func
import logging
import datetime
import json
import uuid
import os
from azure.cosmos import CosmosClient

app = func.FunctionApp()

COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME")

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
