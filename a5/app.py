from flask import Flask, request, render_template
import time
import pandas as pd
from models.Es_Pr import IndexerWithPR
from models.Pr import Pr

app = Flask(__name__)

indexer = IndexerWithPR()
indexer.run_indexer()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search_es_pr', methods=['GET'])
def search_es_pr():
    start = time.time()
    query_term = request.args.get('query', '')

    results = indexer.es_client.search(
        index='simple',
        source_excludes=['url_lists'],
        size=100,
        query={
            "script_score": {
                "query": {"match": {"text": query_term}},
                "script": {"source": "_score * doc['pagerank'].value"}
            }
        }
    )

    end = time.time()
    total_hit = results['hits']['total']['value']

    results_list = [
        {
            'title': hit["_source"]['title'],
            'url': hit["_source"]['url'],
            'text': hit["_source"]['text'][:100]
        }
        for hit in results['hits']['hits']
    ]

    return render_template('search_results.html', query=query_term, results=results_list, total_hit=total_hit, elapse=end - start)

if __name__ == '__main__':
    app.run(debug=False)

