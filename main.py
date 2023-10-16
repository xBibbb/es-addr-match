from elasticsearch import Elasticsearch, helpers
from pathlib import Path
import pandas as pd


def create_index(client: Elasticsearch, index_name: str, synonym_path: str) -> None:
    """
    创建一个index
    :param client: es客户端
    :param index_name: 要创建的index名字
    :param synonym_path: 同义词字典
    :return:
    """
    # 定义映射
    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "char_filter": {
                "remove_special": {
                    "type": "pattern_replace",
                    "pattern": "[&\\-\\(\\)]",  # 是否需要替换 '-' 或者 '/'？
                    "replacement": " "  # 替换为空
                }
            },
            "filter": {
                "synonym_filter": {
                    "type": "synonym",
                    "synonyms_path": synonym_path  # 请确保Elasticsearch可以访问此路径 ["dr=>drive", "rd=>road"]
                }
            },
            "analyzer": {
                "synonym_analyzer": {
                    "type": "custom",
                    "char_filter": "remove_special",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "synonym_filter"]
                },
                "addr_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    }
    mappings = {
        "properties": {
            "rating_authority_name": {
                "type": "text",
                "analyzer": "addr_analyzer"
            },
            "suburb": {
                "type": "text",
                "analyzer": "addr_analyzer",
            },
            "address": {
                "type": "text",
                "analyzer": "synonym_analyzer"
            },
            "property_xcoord": {
                "type": "float",
                "index": False  # 暂时先不创建索引
            },
            "property_ycoord": {
                "type": "float",
                "index": False  # 暂时先不创建索引
            }
        }
    }

    # 创建索引
    if client.indices.exists(index=index_name):
        print(f'Quitting creation procedure, the index {index_name} already exists')
        return
    else:
        client.indices.create(index=index_name, settings=settings, mappings=mappings)

    print(f"Index {index_name} created successfully!")


def delete_index(client: Elasticsearch, index_name: str) -> None:
    """
    删除一个index
    :param client:
    :param index_name:
    :return:
    """
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        print(f"Index {index_name} deleted successfully!")
    else:
        print(f"Index {index_name} does not exist!")


def retrieve_from_bq() -> pd.DataFrame:
    """
    从bigquery取数据，保存到本地文件
    :return:
    """
    from google.cloud import bigquery
    bq = bigquery.Client.from_service_account_json("bigquery_query.json")

    sql = """
    SELECT
      property_id,
      rating_authority_name,
      suburb,
      address,
      property_xcoord,
      property_ycoord
    FROM
      `thave-ai.valocity.valocity_properties`;
    """

    df = bq.query(sql, job_config=bigquery.QueryJobConfig(priority=bigquery.QueryPriority.BATCH)).to_dataframe()
    df.to_csv("tmp.csv", index=False, header=True)

    print("Data downloaded successfully.")
    return df


def load_es(client: Elasticsearch, index_name: str, path: str) -> None:
    """
    上传数据至Elasticsearch
    :return:
    """
    import csv

    def to_float(value):
        """字符串转换为float"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def generate_actions():
        """创建actions"""
        for row in reader:
            for col in ["property_xcoord", "property_ycoord"]:
                row[col] = to_float(row[col])
            yield {
                "_index": index_name,
                "_id": row['property_id'],
                "_source": row
            }

    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        helpers.bulk(client=client, actions=generate_actions())

    print(f'Elasticsearch data loaded successfully! (from: {path})')


def truncate_es(client: Elasticsearch, index_name: str) -> None:
    """
    删除elasticsearch中指定index的所有文档
    :return:
    """

    query = {
        "match_all": {}
    }

    response = client.delete_by_query(index=index_name, query=query)
    print(f"Deleted {response['deleted']} documents from {index_name}")


def search_es(agent_address):
    """
    根据中介提供的地址，返回匹配上的valocity相关数据
    :param agent_address:
    :return:
    """
    pass


def delete_tmp_files(directory: str) -> None:
    """
    删除所有名字中包含tmp的文件
    """
    dir_path = Path(directory)
    for file_path in dir_path.iterdir():
        if file_path.is_file() and 'tmp' in file_path.name:
            file_path.unlink()
            print(f"Deleted: {file_path}")
    print('Done.')


if __name__ == '__main__':
    INDEX_NAME = "valocity"

    # 下载数据
    # data = retrieve_from_bq()

    # 连接到Elasticsearch
    es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

    # 确保Elasticsearch已启动并可访问
    if not es.ping():
        raise ValueError("Connection failed")

    # 创建index
    delete_index(client=es, index_name=INDEX_NAME)
    create_index(client=es, index_name=INDEX_NAME, synonym_path="synonyms.txt")

    # 导入数据
    load_es(client=es, index_name=INDEX_NAME, path="tmp.csv")
