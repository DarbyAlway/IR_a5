import os
import json
import pickle
from pathlib import Path
from elasticsearch import Elasticsearch
from models.Pr import Pr



from elasticsearch.helpers import bulk

class IndexerWithPR:
    def __init__(self):
        self.crawled_folder = Path('C:/Users/user/Documents/Year3_2/IR/resources/crawled')
        self.indexed_data_path = "indexed_data.pkl"
        
        # Load URL mapping
        with open(self.crawled_folder / 'url_list.pickle', 'rb') as f:
            self.file_mapper = pickle.load(f)

        # Connect to Elasticsearch
        self.es_client = Elasticsearch(
            "https://localhost:9200",
            basic_auth=("elastic", "ySGo56ThrQ2moHl+WbG2"),
            ca_certs="~/http_ca.crt"
        )

        with open('a5/pickled/pr_instance.pkl', 'rb') as f:
            self.pr = pickle.load(f)

    def run_indexer(self):
        """Indexes data to Elasticsearch, using a cache to avoid redundant processing."""
        self.es_client.options(ignore_status=[400, 404]).indices.delete(index='simple')
        self.es_client.options(ignore_status=[400]).indices.create(index='simple')

        if os.path.exists(self.indexed_data_path):
            print("Loading indexed data from pickle file...")
            with open(self.indexed_data_path, "rb") as f:
                indexed_data = pickle.load(f)
        else:
            print("Processing files and creating indexed data...")
            indexed_data = []
            for file in os.listdir(self.crawled_folder):
                if file.endswith(".txt"):
                    with open(os.path.join(self.crawled_folder, file), 'r',encoding='utf-8') as f:
                        j = json.load(f)
                    j['id'] = j['url']
                    j['pagerank'] = self.pr.pr_result.loc[j['id']].score
                    indexed_data.append(j)

            # Save processed data to a pickle file
            with open(self.indexed_data_path, "wb") as f:
                pickle.dump(indexed_data, f)

        # Prepare documents for bulk indexing
        actions = []
        for doc in indexed_data:
            action = {
                "_op_type": "index",  # This can also be "create" if you only want to index new documents
                "_index": "simple",
                "_id": doc["id"],  # Optional, you can specify an id or let Elasticsearch auto-generate it
                "_source": doc
            }
            actions.append(action)

        # Use Elasticsearch's bulk helper function to index all documents at once
        print("Sending bulk index to Elasticsearch...")
        success, failed = bulk(self.es_client, actions)
        print(f"Bulk indexing complete. {success} documents indexed, {failed} failed.")

