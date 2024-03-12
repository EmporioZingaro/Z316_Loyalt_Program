import os
import json
import re
import logging

from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
DATASET_ID = os.environ.get('DATASET_ID')
TABLE_ID_SALES = os.environ.get('TABLE_ID_SALES')
TABLE_ID_SALES_ITEMS = os.environ.get('TABLE_ID_SALES_ITEMS')
SOURCE_IDENTIFIER = os.environ.get('SOURCE_IDENTIFIER')
VERSION_CONTROL = os.environ.get('VERSION_CONTROL')
FIDELIDADE_MULTIPLIER = float(os.environ.get('FIDELIDADE_MULTIPLIER', 0.0))
MORNING_MULTIPLIER = float(os.environ.get('MORNING_MULTIPLIER', 0.0))
HAPPYHOUR_MULTIPLIER = float(os.environ.get('HAPPYHOUR_MULTIPLIER', 0.0))

sales_schema = [
    bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pedido_dia", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pedido_numero", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_cpf", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_valor", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("fidelidade_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("special_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("final_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("pedido_pontos", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_identifier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version_control", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="REQUIRED"),
]

sales_items_schema = [
    bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_idProduto", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pedido_dia", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pedido_numero", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_cpf", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_descricao", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_category_first", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_category_second", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_quantidade", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_valor", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_valor_total", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_pontos_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("fidelidade_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("special_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("final_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_pontos_total", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_identifier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version_control", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="REQUIRED"),
]

sales_primary_key = ["uuid", "pedido_id"]
sales_items_primary_key = ["uuid", "pedido_id", "produto_idProduto"]

storage_client = storage.Client()
bigquery_client = bigquery.Client()
subscriber = pubsub_v1.SubscriberClient()


def convert_to_sao_paulo_time(utc_timestamp: datetime) -> datetime:
    """
    Convert a UTC timestamp to Sao Paulo timezone.

    Args:
        utc_timestamp (datetime): The UTC timestamp to convert.

    Returns:
        datetime: The timestamp converted to Sao Paulo timezone.
    """
    sao_paulo_timezone = ZoneInfo("America/Sao_Paulo")
    sao_paulo_timestamp = utc_timestamp.astimezone(sao_paulo_timezone)
    return sao_paulo_timestamp


def extract_multiplier(obs: str) -> float:
    """
    Extract the lowest multiplier from the observation string.

    Args:
        obs (str): The observation string containing multipliers.

    Returns:
        float: The lowest multiplier found in the observation string, or 0.0 if no multiplier is found.
    """
    logger.debug(f"Extracting multiplier from obs: {obs}")
    matches = re.findall(r'{{(\d+\.\d+)}}', obs)
    if matches:
        multipliers = [float(match) for match in matches]
        lowest_multiplier = min(multipliers)
        logger.debug(f"Extracted multipliers: {multipliers}")
        logger.debug(f"Lowest multiplier: {lowest_multiplier}")
        logger.warning(f"Found multiple multipliers in obs: {multipliers}")
        return lowest_multiplier
    logger.debug("No multiplier found in obs")
    return 0.0


def get_special_multiplier(processing_timestamp: datetime) -> float:
    """
    Get the special multiplier based on the processing timestamp.

    Args:
        processing_timestamp (datetime): The processing timestamp.

    Returns:
        float: The special multiplier based on the processing timestamp.
    """
    logger.debug(f"Getting special multiplier for timestamp: {processing_timestamp}")
    sao_paulo_timestamp = convert_to_sao_paulo_time(processing_timestamp)
    purchase_time = sao_paulo_timestamp.time()
    purchase_weekday = sao_paulo_timestamp.weekday()
    logger.debug(f"Purchase time: {purchase_time}, Purchase weekday: {purchase_weekday}")

    special_multipliers = []

    # Rule 1: Apply morning multiplier for purchases made between 5 AM and 10 AM
    if datetime.time(5, 0, 0) <= purchase_time < datetime.time(10, 0, 0):
        special_multipliers.append(MORNING_MULTIPLIER)
        logger.info(f"Applying morning multiplier: {MORNING_MULTIPLIER}")

    # Rule 2: Apply happy hour multiplier for purchases made on Fridays between 6 PM and 10 PM
    if purchase_weekday == 4 and datetime.time(18, 0, 0) <= purchase_time < datetime.time(22, 0, 0):
        special_multipliers.append(HAPPYHOUR_MULTIPLIER)
        logger.info(f"Applying happy hour multiplier: {HAPPYHOUR_MULTIPLIER}")

    if special_multipliers:
        total_multiplier = sum(special_multipliers)
        logger.info(f"Total special multiplier: {total_multiplier}")
        return total_multiplier
    else:
        logger.debug("No special multiplier applicable")
        return 0.0


def download_json_file(blob: storage.Blob) -> Optional[dict]:
    """
    Download and parse a JSON file from Google Cloud Storage.

    Args:
        blob (storage.Blob): The blob representing the JSON file.

    Returns:
        Optional[dict]: The parsed JSON data, or None if an error occurs.
    """
    logger.debug(f"Downloading JSON file: {blob.name}")
    try:
        json_data = json.loads(blob.download_as_string())
        logger.debug(f"Successfully downloaded and parsed JSON file: {blob.name}")
        return json_data
    except (storage.exceptions.NotFound, json.JSONDecodeError) as exception:
        logger.error(f"Error downloading or parsing file: {blob.name}. Error: {str(exception)}")
        return None


def download_json_files(uuid: str) -> tuple:
    """
    Download JSON files from Google Cloud Storage based on the provided UUID.

    Args:
        uuid (str): The UUID of the files to download.

    Returns:
        tuple: A tuple containing the downloaded data (pesquisa_data, pdv_data, produto_data, processing_timestamp).
    """
    logger.info(f"Downloading JSON files for UUID: {uuid}")
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(fields='items/metadata')
    data = {
        'pesquisa': None,
        'pdv': None,
        'produto': {},
        'processing_timestamp': None
    }

    for blob in blobs:
        metadata = blob.metadata
        if metadata and metadata.get('UUID') == uuid:
            logger.debug(f"Found blob with matching UUID: {blob.name}")
            data_type = metadata['Data-Type']
            if data_type == 'pedidos.pesquisa':
                logger.debug(f"Downloading pesquisa data: {blob.name}")
                data['pesquisa'] = download_json_file(blob)
            elif data_type == 'pdv.pedido':
                logger.debug(f"Downloading PDV data: {blob.name}")
                data['pdv'] = download_json_file(blob)
                processing_timestamp_str = metadata.get('Processing-Timestamp')
                if processing_timestamp_str:
                    data['processing_timestamp'] = datetime.fromisoformat(processing_timestamp_str.replace('Z', '+00:00'))
                    logger.debug(f"Processing timestamp extracted from PDV blob: {data['processing_timestamp']}")
            elif data_type == 'produto':
                produto_id = metadata['Produto-ID']
                logger.debug(f"Downloading produto data: {blob.name}, Produto-ID: {produto_id}")
                data['produto'][produto_id] = download_json_file(blob)

    if not data['processing_timestamp']:
        logger.warning("Processing timestamp not found in PDV blob metadata")

    logger.info(f"Finished downloading JSON files for UUID: {uuid}")
    return data['pesquisa'], data['pdv'], data['produto'], data['processing_timestamp']


def extract_pesquisa_data(pesquisa_data: dict) -> tuple:
    """
    Extract relevant data from the pesquisa JSON data.

    Args:
        pesquisa_data (dict): The pesquisa JSON data.

    Returns:
        tuple: A tuple containing the extracted data (nome_vendedor, id_vendedor).
    """
    logger.debug(f"Extracting pesquisa data: {pesquisa_data}")
    try:
        pedido = pesquisa_data['retorno']['pedidos'][0]['pedido']
        nome_vendedor = pedido['nome_vendedor']
        id_vendedor = pedido['id_vendedor']
        logger.debug(f"Extracted pesquisa data - Nome Vendedor: {nome_vendedor}, ID Vendedor: {id_vendedor}")
        return nome_vendedor, id_vendedor
    except (KeyError, IndexError) as exception:
        logger.error(f"Error extracting pesquisa data: {str(exception)}")
        return None, None


def extract_pdv_data(pdv_data: dict) -> tuple:
    """
    Extract relevant data from the PDV JSON data.

    Args:
        pdv_data (dict): The PDV JSON data.

    Returns:
        tuple: A tuple containing the extracted data (pedido_id, pedido_numero, pedido_dia, total_produtos, total_venda, observacoes, forma_pagamento, cliente_nome, cliente_cpf).
    """
    logger.debug(f"Extracting PDV data: {pdv_data}")
    try:
        pedido_pdv = pdv_data['retorno']['pedido']
        pedido_id = pedido_pdv['id']
        pedido_numero = pedido_pdv['numero']
        pedido_dia = datetime.strptime(pedido_pdv['data'], '%d/%m/%Y').strftime('%Y-%m-%d')
        total_produtos = float(pedido_pdv['totalProdutos'])
        total_venda = float(pedido_pdv['totalVenda'])
        observacoes = pedido_pdv['observacoes']
        forma_pagamento = pedido_pdv['formaPagamento']
        cliente_nome = pedido_pdv['contato']['nome']
        cliente_cpf = pedido_pdv['contato']['cpfCnpj']
        logger.debug(f"Extracted PDV data - Pedido ID: {pedido_id}, Pedido Numero: {pedido_numero}, Pedido Dia: {pedido_dia}, Total Produtos: {total_produtos}, Total Venda: {total_venda}, Observacoes: {observacoes}, Forma Pagamento: {forma_pagamento}, Cliente Nome: {cliente_nome}, Cliente CPF: {cliente_cpf}")
        return pedido_id, pedido_numero, pedido_dia, total_produtos, total_venda, observacoes, forma_pagamento, cliente_nome, cliente_cpf
    except (KeyError, ValueError) as exception:
        logger.error(f"Error extracting PDV data: {str(exception)}")
        return None, None, None, None, None, None, None, None, None


def process_pedido_item(item: dict, produto_data: dict, processing_timestamp: datetime, pedido_dia: str, uuid: str, nome_vendedor: str, id_vendedor: str, cliente_nome: str, cliente_cpf: str) -> tuple:
    """
    Process a single pedido item and extract relevant data.

    Args:
        item (dict): The pedido item data.
        produto_data (dict): The produto data dictionary.
        processing_timestamp (datetime): The processing timestamp.
        pedido_dia (str): The pedido dia.
        uuid (str): The UUID.
        nome_vendedor (str): The nome do vendedor.
        id_vendedor (str): The ID do vendedor.
        cliente_nome (str): The nome do cliente.
        cliente_cpf (str): The CPF do cliente.

    Returns:
        tuple: A tuple containing the processed data (produto_valor_total, produto_pontos, sales_items_row).
    """
    logger.debug(f"Processing pedido item: {item}")
    produto_id = item['idProduto']
    produto = produto_data.get(produto_id)
    if produto:
        produto_descricao = item.get('descricao', '')
        produto_quantidade = float(item.get('quantidade', 0))
        produto_valor = float(item.get('valor', 0))
        produto_desconto = float(item.get('desconto', 0))
        produto_preco_custo = float(produto['produto'].get('preco_custo', 0))
        produto_obs = produto['produto'].get('obs', '')
        produto_categoria = produto['produto'].get('categoria', '')
        produto_multiplier = extract_multiplier(produto_obs)
        special_multiplier = get_special_multiplier(processing_timestamp)
        final_multiplier = FIDELIDADE_MULTIPLIER + special_multiplier + produto_multiplier
        produto_valor_total = produto_quantidade * produto_valor
        produto_pontos = produto_valor_total * final_multiplier
        categoria_split = produto_categoria.split(' >> ')
        produto_category_first = categoria_split[0] if len(categoria_split) > 0 else ''
        produto_category_second = categoria_split[1] if len(categoria_split) > 1 else ''
        sales_items_row = {
            'uuid': uuid,
            'timestamp': convert_to_sao_paulo_time(processing_timestamp),
            'pedido_dia': pedido_dia,
            'pedido_id': item['pedido_id'],
            'pedido_numero': item['pedido_numero'],
            'cliente_nome': cliente_nome,
            'cliente_cpf': cliente_cpf,
            'vendedor_nome': nome_vendedor,
            'vendedor_id': id_vendedor,
            'produto_idProduto': produto_id,
            'produto_descricao': produto_descricao,
            'produto_category_first': produto_category_first,
            'produto_category_second': produto_category_second,
            'produto_quantidade': produto_quantidade,
            'produto_valor': produto_valor,
            'produto_valor_total': produto_valor_total,
            'produto_multiplier': produto_multiplier,
            'fidelidade_multiplier': FIDELIDADE_MULTIPLIER,
            'special_multiplier': special_multiplier,
            'final_multiplier': final_multiplier,
            'produto_pontos': produto_pontos,
            'project_id': PROJECT_ID,
            'source_identifier': SOURCE_IDENTIFIER,
            'version_control': VERSION_CONTROL,
            'processed_timestamp': datetime.now(tz=ZoneInfo("America/Sao_Paulo")).isoformat()
        }
        logger.debug(f"Processed pedido item - Produto ID: {produto_id}, Produto Descricao: {produto_descricao}, Produto Quantidade: {produto_quantidade}, Produto Valor: {produto_valor}, Produto Valor Total: {produto_valor_total}, Produto Multiplier: {produto_multiplier}, Special Multiplier: {special_multiplier}, Final Multiplier: {final_multiplier}, Produto Pontos: {produto_pontos}")
        return produto_valor_total, produto_pontos, sales_items_row
    else:
        logger.warning(f"Produto not found for Produto ID: {produto_id}")
    return 0.0, 0.0, None


def process_pedido_items(pedido_pdv: dict, produto_data: dict, processing_timestamp: datetime, pedido_dia: str, uuid: str, nome_vendedor: str, id_vendedor: str, cliente_nome: str, cliente_cpf: str) -> tuple:
    """
    Process all pedido items and extract relevant data.

    Args:
        pedido_pdv (dict): The PDV pedido data.
        produto_data (dict): The produto data dictionary.
        processing_timestamp (datetime): The processing timestamp.
        pedido_dia (str): The pedido dia.
        uuid (str): The UUID.
        nome_vendedor (str): The nome do vendedor.
        id_vendedor (str): The ID do vendedor.
        cliente_nome (str): The nome do cliente.
        cliente_cpf (str): The CPF do cliente.

    Returns:
        tuple: A tuple containing the processed data (pedido_valor, pedido_pontos, sales_items_rows).
    """
    logger.info(f"Processing pedido items for UUID: {uuid}")
    sales_items_rows = []
    for item in pedido_pdv['itens']:
        item['pedido_id'] = pedido_pdv['id']
        item['pedido_numero'] = pedido_pdv['numero']
        produto_valor_total, produto_pontos, sales_items_row = process_pedido_item(
            item, produto_data, processing_timestamp, pedido_dia, uuid, nome_vendedor, id_vendedor, cliente_nome, cliente_cpf
        )
        if sales_items_row:
            sales_items_rows.append(sales_items_row)

    pedido_valor = sum(row['produto_valor_total'] for row in sales_items_rows)
    pedido_pontos = sum(row['produto_pontos'] for row in sales_items_rows)
    logger.debug(f"Processed pedido items - Pedido Valor: {pedido_valor}, Pedido Pontos: {pedido_pontos}, Sales Items Rows: {len(sales_items_rows)}")
    return pedido_valor, pedido_pontos, sales_items_rows


def prepare_sales_row(uuid: str, processing_timestamp: datetime, pedido_dia: str, pedido_pdv: dict, nome_vendedor: str, id_vendedor: str, cliente_nome: str, cliente_cpf: str, pedido_valor: float, pedido_pontos: float) -> dict:
    """
    Prepare the sales row data.

    Args:
        uuid (str): The UUID.
        processing_timestamp (datetime): The processing timestamp.
        pedido_dia (str): The pedido dia.
        pedido_pdv (dict): The PDV pedido data.
        nome_vendedor (str): The nome do vendedor.
        id_vendedor (str): The ID do vendedor.
        cliente_nome (str): The nome do cliente.
        cliente_cpf (str): The CPF do cliente.
        pedido_valor (float): The pedido valor.
        pedido_pontos (float): The pedido pontos.

    Returns:
        dict: The prepared sales row data.
    """
    logger.debug(f"Preparing sales row for UUID: {uuid}")
    sales_row = {
        'uuid': uuid,
        'timestamp': convert_to_sao_paulo_time(processing_timestamp),
        'pedido_dia': pedido_dia,
        'pedido_id': pedido_pdv['id'],
        'pedido_numero': pedido_pdv['numero'],
        'cliente_nome': cliente_nome,
        'cliente_cpf': cliente_cpf,
        'vendedor_nome': nome_vendedor,
        'vendedor_id': id_vendedor,
        'pedido_valor': pedido_valor,
        'fidelidade_multiplier': FIDELIDADE_MULTIPLIER,
        'pedido_pontos': pedido_pontos,
        'project_id': PROJECT_ID,
        'source_identifier': SOURCE_IDENTIFIER,
        'version_control': VERSION_CONTROL,
        'processed_timestamp': datetime.now(tz=ZoneInfo("America/Sao_Paulo")).isoformat()
    }
    logger.debug(f"Prepared sales row: {sales_row}")
    return sales_row


def create_dataset_if_not_exists(dataset_id: str) -> None:
    """
    Create a BigQuery dataset if it doesn't exist.

    Args:
        dataset_id (str): The ID of the dataset to create.
    """
    logger.debug(f"Checking if dataset exists: {dataset_id}")
    try:
        bigquery_client.get_dataset(dataset_id)
        logger.info(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        logger.info(f"Dataset '{dataset_id}' not found, creating it.")
        dataset = bigquery.Dataset(dataset_id)
        bigquery_client.create_dataset(dataset)
        logger.info(f"Created dataset '{dataset_id}'.")


def create_table_if_not_exists(table_id: str, schema: List[bigquery.SchemaField]) -> None:
    """
    Create a BigQuery table if it doesn't exist.

    Args:
        table_id (str): The ID of the table to create.
        schema (List[bigquery.SchemaField]): The schema of the table.
    """
    logger.debug(f"Checking if table exists: {table_id}")
    try:
        bigquery_client.get_table(table_id)
        logger.info(f"Table '{table_id}' already exists.")
    except NotFound:
        logger.info(f"Table '{table_id}' not found, creating it.")
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="pedido_dia"
        )
        if table_id.endswith(TABLE_ID_SALES):
            table.clustering_fields = ["cliente_cpf", "vendedor_id"]
            table.schema = sales_schema
        elif table_id.endswith(TABLE_ID_SALES_ITEMS):
            table.clustering_fields = ["cliente_cpf", "vendedor_id", "produto_idProduto"]
            table.schema = sales_items_schema

            foreign_key = bigquery.ForeignKeyReference(
                bigquery.TableReference.from_string(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_SALES}"),
                ["uuid", "pedido_id"],
                ["uuid", "pedido_id"]
            )
            table.schema.append(bigquery.SchemaField("sales_foreign_key", "RECORD", mode="NULLABLE", fields=foreign_key.to_api_repr()["fields"]))

        bigquery_client.create_table(table)
        logger.info(f"Created table '{table_id}' with partitioning, clustering, primary key, and foreign key (if applicable).")


def load_data_to_bigquery(sales_row: dict, sales_items_rows: List[dict], uuid: str) -> None:
    """
    Load data to BigQuery tables.

    Args:
        sales_row (dict): The sales row data.
        sales_items_rows (List[dict]): The sales items rows data.
        uuid (str): The UUID.
    """
    logger.info(f"Loading data to BigQuery for UUID: {uuid}")
    try:
        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
        sales_table_id = f"{dataset_id}.{TABLE_ID_SALES}"
        sales_items_table_id = f"{dataset_id}.{TABLE_ID_SALES_ITEMS}"
        create_dataset_if_not_exists(dataset_id)
        create_table_if_not_exists(sales_table_id, sales_schema)
        create_table_if_not_exists(sales_items_table_id, sales_items_schema)

        sales_table = bigquery_client.get_table(sales_table_id)
        sales_table.schema = sales_schema
        sales_table.primary_key = sales_primary_key
        bigquery_client.update_table(sales_table, ["schema", "primary_key"])

        sales_items_table = bigquery_client.get_table(sales_items_table_id)
        sales_items_table.schema = sales_items_schema
        sales_items_table.primary_key = sales_items_primary_key
        bigquery_client.update_table(sales_items_table, ["schema", "primary_key"])

        errors = bigquery_client.insert_rows_json(sales_table_id, [sales_row])
        if errors:
            logger.error(f"Errors while inserting sales data: {errors}")
        else:
            logger.info(f"Sales data inserted successfully for UUID: {uuid}")

        errors = bigquery_client.insert_rows_json(sales_items_table_id, sales_items_rows)
        if errors:
            logger.error(f"Errors while inserting sales items data: {errors}")
        else:
            logger.info(f"Sales items data inserted successfully for UUID: {uuid}")
    except Exception as exception:
        logger.exception(f"Error loading data to BigQuery: {str(exception)}")


def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Process a Pub/Sub message.

    Args:
        message (pubsub_v1.subscriber.message.Message): The Pub/Sub message to process.
    """
    uuid = message.data.decode('utf-8')
    logger.info(f"Processing message for UUID: {uuid}")
    try:
        pesquisa_data, pdv_data, produto_data, processing_timestamp = download_json_files(uuid)
        if pesquisa_data and pdv_data and processing_timestamp:
            nome_vendedor, id_vendedor = extract_pesquisa_data(pesquisa_data)
            pedido_id, pedido_numero, pedido_dia, total_produtos, total_venda, observacoes, forma_pagamento, cliente_nome, cliente_cpf = extract_pdv_data(pdv_data)
            if nome_vendedor and id_vendedor and pedido_id and pedido_numero and pedido_dia:
                pedido_valor, pedido_pontos, sales_items_rows = process_pedido_items(
                    pdv_data['retorno']['pedido'], produto_data, processing_timestamp, pedido_dia, uuid, nome_vendedor, id_vendedor, cliente_nome, cliente_cpf
                )
                sales_row = prepare_sales_row(uuid, processing_timestamp, pedido_dia, pdv_data['retorno']['pedido'], nome_vendedor, id_vendedor, cliente_nome, cliente_cpf, pedido_valor, pedido_pontos)
                load_data_to_bigquery(sales_row, sales_items_rows, uuid)
            else:
                logger.warning(f"Incomplete data for UUID: {uuid}")
        else:
            logger.warning(f"Missing pesquisa, PDV data or Processing Timestamp for UUID: {uuid}")
    except Exception as exception:
        logger.exception(f"Error processing message: {str(exception)}")
    message.ack()


def pubsub_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Callback function for processing Pub/Sub messages.

    Args:
        message (pubsub_v1.subscriber.message.Message): The Pub/Sub message received.
    """
    logger.info(f"Received message: {message.data}")
    try:
        attributes = message.attributes
        processing_timestamp_str = attributes.get('Processing-Timestamp')
        if processing_timestamp_str:
            processing_timestamp = datetime.fromisoformat(processing_timestamp_str.replace('Z', '+00:00'))
            process_message(message, processing_timestamp)
        else:
            logger.warning(f"Processing-Timestamp not found in message attributes: {message.data}")
            message.ack()
    except Exception as exception:
        logger.exception(f"Error processing message: {message.data}")
        message.ack()


def main(event: dict, context: Any) -> None:
    """
    The main function to start the processing.

    Args:
        event (dict): The event data.
        context (Any): The context data.
    """
    logger.info("Starting the main function")
    project_id = PROJECT_ID
    subscription_id = 'api-to-gcs_DONE-sub'
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    logger.info(f"Listening for messages on {subscription_path}..\n")
    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
    logger.info("Exiting the main function")
