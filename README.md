# opc-iot-demo
このデモアプリは以下を実行します。
- Azure IoT Hub で受け取った MQTT メッセージを Cosmos DB に保存
- 保存された Cosmos DB のデータを Azure OpenAI Service や Azure Machine Learning に渡して得られた出力結果を Cosmos DB に保存
- タイマートリガーにて、Cosmos DB に保存されているデータを Azure Data Explorer Database に保存

## 利用手順
### ローカル実行
`local.settings.json` の更新

### Azure での実行
Azure ポータルで以下の環境変数の設定
- `COSMOS_CONNECTION_STRING`
- `COSMOS_DATABASE_NAME`
- `COSMOS_CONTAINER_NAME`
