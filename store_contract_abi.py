# script to get the ABI of a contract from Etherscan and save it to BigQuery

# you need to install the following packages
# pip3 install requests pysha3 google-cloud-bigquery pytz
import requests
import json
import sha3
import time
import logging
import sys


from google.cloud import bigquery
from functools import lru_cache

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# https://eips.ethereum.org/EIPS/eip-1967
_IMPLEMENTATION_SLOTS = [
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc",
    "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3",
]
PROXY_TOPIC = "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"

POLYGONSCAN_API_KEY = "GXFI7FNMQUJEGW2S5QX1BADTYAKTZNRN1S"
BNBSCAN_API_KEY = "XCDV5GZMGHYADFX437Q51MYB35V1TRIH5X"
AVALANCHE_API_KEY = "HAJ1CWWQYAJ22E45ECVZ4VVZQRA98KQWDT"
ARBITRUM_API_KEY = "TX4F1E8HXFBUY83TXYMQ8M62HUG2FVTU4A"
OPTIMISM_API_KEY = "WQIF1P342TMPTBSWJ11FM1AHWVX3X75UEN"
FANTOM_API_KEY = "TMU1QRJFN7GRU4BX8CIH35KJXPKGBR854M"
ETHERSCAN_API_KEY = "68FEKI7EHR2JDCAT1S6QAEB82WPYUX8JFE"#K7JNYY2KP1HUATMV2FRJ298X7WJWSYJS32"


class AbiReader:
    def __init__(self):
        self.block_explorer_map = {
            "ethereum": "https://api.etherscan.io",
            "polygon": "https://api.polygonscan.com",
            "binance": "https://api.bscscan.com",
            "avalanche": "https://api.snowtrace.io",
            "arbitrum": "https://api.arbiscan.io",
            "optimism": "https://api.optimistic.etherscan.io",
            "fantom": "https://api.ftmscan.com",
        }
        self.api_keys = {
            "ethereum": ETHERSCAN_API_KEY,
            "polygon": POLYGONSCAN_API_KEY,
            "binance": BNBSCAN_API_KEY,
            "avalanche": AVALANCHE_API_KEY,
            "arbitrum": ARBITRUM_API_KEY,
            "optimism": OPTIMISM_API_KEY,
            "fantom": FANTOM_API_KEY,
        }
        self.local_abi_cache = []
        self.bq_client = bigquery.Client()
        self.bq_project = "circle-ds-pipelines"

    @lru_cache(maxsize=128)
    def get_proxy(self, blockchain, contract_address):
        time.sleep(0.2)
        block_explorer_url = self.block_explorer_map[blockchain]

        url = f"{block_explorer_url}/api?module=proxy&action=eth_getStorageAt"
        for slot in _IMPLEMENTATION_SLOTS:
            params = {
                "address": contract_address.lower(),
                "position": slot,
                "tag": "latest",
                "apikey": self.api_keys[blockchain],
            }
            r = requests.get(url, params=params, verify=False)
            proxy_address = json.loads(r.text)["result"]
            proxy_address = "0x" + proxy_address[-40:].lower()
            if proxy_address == "0x0000000000000000000000000000000000000000":
                continue
            return proxy_address

    def get_topics_dict(self, abi):
        topics = {}

        for event in abi:
            event_name = event.get("name")
            if event_name:
                inputs = [x["type"] for x in event["inputs"]]

                # Build string for Keccak hash
                s = f'{event_name}({",".join(inputs)})'
                k = sha3.keccak_256()  # Generate Keccak hash
                k.update(s.encode("utf-8"))  # Encode to binary

                topic = "0x" + k.hexdigest()
                topics[topic] = {"name": event_name, "inputs": event}

        return topics

    @lru_cache(maxsize=128)
    def _get_abi_from_explorer(self, blockchain: str, contract_address: str):
        """get ABI from explorer"""

        explorer_url = self.block_explorer_map[blockchain]
        url = f"{explorer_url}/api?module=contract&action=getabi"
        params = {
            "address": contract_address,
            "apikey": self.api_keys[blockchain],
        }
        response = requests.get(url=url, params=params, verify=False)
        time.sleep(0.21)
        # print(response.url)
        if response.status_code == 200:
            data = response.json()

            if data["message"] == "NOTOK":
                logging.warning(f"{data['result']} for contract = {contract_address}")
                return {}

            logging.info(
                f"ABI for contract {contract_address} retrieved from {explorer_url}"
            )
            contract_abi = json.loads(data["result"])
            return contract_abi
        else:
            logging.info(f"{response.text}")
            raise Exception("Block explorer API request failed")

    def get_abi_from_explorer(self, blockchain: str, contract_address: str):
        """get ABI from block explorer"""
        contract_abi = self._get_abi_from_explorer(blockchain, contract_address)
        topics = self.get_topics_dict(contract_abi)

        if PROXY_TOPIC in topics or len(topics) == 0:
            logging.info(f"Detected proxy contract for {contract_address}")
            logging.info(topics)
            implementation_address = self.get_proxy(blockchain, contract_address)
            logging.info(f"Proxy implementation address = {implementation_address}")
            abi = self._get_abi_from_explorer(blockchain, implementation_address)
            topics = self.get_topics_dict(abi)

        for topic in topics:
            topic_inputs = topics[topic]["inputs"]
            params = topic_inputs["inputs"]

            topic_length = 1
            data_length = 2
            if topics[topic]["inputs"]:
                for param in params:
                    if param.get("indexed"):
                        topic_length += 1
                    else:
                        data_length += 64

            if topic_length > 1 or data_length > 2:
                results = {
                    "topic": topic,
                    "source_contract": contract_address,
                    "name": topics[topic]["name"],
                    "abi": json.dumps(topic_inputs),
                    "topic_length": topic_length,
                    "data_length": data_length,
                }
                self.local_abi_cache.append(results)
                logging.info(
                    f'ABI for topic {topics[topic]["name"]}: {topic} cached locally'
                )
        return

    def _get_temp_table(
        self, dataset: str, table_name: str = None, project=None
    ) -> bigquery.Table:
        import random
        import datetime
        import pytz

        prefix = "temp"
        suffix = random.randint(10000, 99999)

        if not table_name:
            table_name = "noname"

        temp_table_name = f"{dataset}.{prefix}_{table_name}_{suffix}"
        if project:
            temp_table_name = f"{project}.{temp_table_name}"
        tmp_table_def = bigquery.Table(temp_table_name)
        tmp_table_def.expires = datetime.datetime.now(pytz.utc) + datetime.timedelta(
            minutes=30
        )

        return tmp_table_def

    def upload_to_bigquery(self):
        "create a temp table and then merge into the main table"
        dataset_id = "sxu"#ethereum

        # create temp table
        tmp_table_def = self._get_temp_table(dataset_id, "topics", self.bq_project)

        tmp_table_def.schema = [
            bigquery.SchemaField("topic", "STRING"),
            bigquery.SchemaField("source_contract", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("abi", "STRING"),
            bigquery.SchemaField("topic_length", "INTEGER"),
            bigquery.SchemaField("data_length", "INTEGER"),
        ]
        table = self.bq_client.create_table(tmp_table_def)
        logging.info(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )

        # insert rows into temp table
        errors = self.bq_client.insert_rows_json(table, self.local_abi_cache)
        if errors == []:
            logging.info("New rows have been added.")
        else:
            logging.error("Encountered errors while inserting rows: {}".format(errors))

        # merge into main table
        query = f"""
        MERGE `utils.topic_abis` AS target
        USING `{table.project}.{table.dataset_id}.{table.table_id}` AS source
            ON target.topic = source.topic
            AND target.topic_length = source.topic_length
            AND target.data_length = source.data_length
        WHEN MATCHED THEN
            UPDATE SET
                source_contract = source.source_contract,
                name = source.name,
                abi = source.abi
        WHEN NOT MATCHED THEN
            INSERT (topic, source_contract, name, abi, topic_length, data_length)
            VALUES (source.topic, source.source_contract, source.name,
                source.abi, source.topic_length, source.data_length);
        """
        query_job = self.bq_client.query(query)
        query_job.result()
        logging.info("Table merged")


def main(blockchain, contract_address):
    logging.info(f"Processing contract {contract_address} on {blockchain}")
    reader = AbiReader()
    reader.get_abi_from_explorer(blockchain, contract_address)
    reader.upload_to_bigquery()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
