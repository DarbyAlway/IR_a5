import os
import json
import pickle
from pathlib import Path
from elasticsearch import Elasticsearch, helpers

class TfIdfRanker:
    def __init__(self):
        self.crawled_folder = Path('resources/crawled')
        
        # Load file_mapper for file paths
        with open(self.crawled_folder / 'url_list.pickle', 'rb') as f:
            self.file_mapper = pickle.load(f)
        
        # Initialize Elasticsearch client
        self.es_client = Elasticsearch("https://localhost:9200", 
                                       basic_auth=("elastic", "ySGo56ThrQ2moHl+WbG2"), 
                                       ca_certs="~/http_ca.crt")
        
        # Load PageRank instance
        with open('pr_instance.pkl', 'rb') as f:
            self.pr = pickle.load(f)

    def run_indexer(self):
        pickle_file = 'indexed_documents.pickle'
        
        # Check if the pickle file exists
        if os.path.exists(pickle_file):
            # Load the pickled data if file exists
            with open(pickle_file, 'rb') as f:
                indexed_documents = pickle.load(f)
            print(f"Loaded indexed documents from {pickle_file}")
        else:
            # If pickle file doesn't exist, perform the indexing
            # Delete and create the index
            self.es_client.options(ignore_status=[400, 404]).indices.delete(index='extend')
            self.es_client.options(ignore_status=[400]).indices.create(index='extend')

            # Initialize an empty list to store indexed documents
            indexed_documents = []
            bulk_actions = []

            for file in os.listdir(self.crawled_folder):
                if file.endswith(".txt"):
                    with open(os.path.join(self.crawled_folder, file), 'r', encoding='utf-8') as f:
                        j = json.load(f)
                    j['id'] = j['url']

                    # Get PageRank score, ensure the key exists
                    pagerank_score = self.pr.pr_result.loc[j['id']].score
                    j['pagerank'] = pagerank_score

                    # Fetch TF-IDF score (Assume it's fetched via a search query)
                    search_result = self.es_client.search(index="extend", body={
                        "query": {
                            "match": {
                                "url": j['url']
                            }
                        },
                        "explain": True  # Enable explanation to get TF-IDF calculation
                    })

                    # Extract TF-IDF score from the search explanation
                    if search_result['hits']['hits']:
                        explain = search_result['hits']['hits'][0]['_explanation']
                        tfidf_score = explain['value']
                    else:
                        tfidf_score = 1  

                    final_score = pagerank_score + tfidf_score
                    j['final_score'] = final_score
                    j['tfidf_score'] = tfidf_score

                    # Prepare document for bulk indexing
                    action = {
                        "_op_type": "index",  # This will index the document
                        "_index": "extend",   # Specify the index name
                        "_id": j['id'],       # Document ID
                        "_source": j          # Document content
                    }
                    bulk_actions.append(action)

                    # Add the document to the list of indexed documents
                    indexed_documents.append(j)

                    print(f"Prepared {j['url']} for bulk indexing with PageRank: {pagerank_score}, TF-IDF: {tfidf_score}, Final Score: {final_score}")
            
            # After preparing all documents, perform the bulk indexing
            if bulk_actions:
                helpers.bulk(self.es_client, bulk_actions)
                print("Bulk indexing completed")

            # After the indexing is done, pickle the list of indexed documents
            with open(pickle_file, 'wb') as f:
                pickle.dump(indexed_documents, f)
            print(f"Pickled indexed documents to {pickle_file}")
