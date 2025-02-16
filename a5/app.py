from flask import Flask, request, render_template
import time
import pandas as pd
from models.Es_Pr import IndexerWithPR
from models.Pr import Pr
from models.TfIdfRanker import TfIdfRanker
import re

app = Flask(__name__)

# Initialize the Elasticsearch indexers
es_pr_indexer = IndexerWithPR()
es_pr_indexer.run_indexer()  # Corrected to call the method

tfidf_indexer = TfIdfRanker()
tfidf_indexer.run_indexer()  # Added parentheses to call the method


def highlight_query(text, query, context=50, max_snippets=3):
    """Highlight all matched query terms and show short previews."""
    pattern = re.compile(rf'\b{re.escape(query)}\b', re.IGNORECASE)  # Match whole word
    matches = list(pattern.finditer(text))  # Find all matches
    
    if not matches:
        return text[:200] + "..."  # Fallback if no match

    snippets = []
    for match in matches[:max_snippets]:  # Limit the number of snippets
        start = max(0, match.start() - context)
        end = min(len(text), match.end() + context)
        snippet = text[start:end]  # Extract relevant portion
        highlighted = pattern.sub(r'<b>\g<0></b>', snippet)  # Apply <mark> tags
        snippets.append(highlighted)
    print(snippets)
    return " ... ".join(snippets) + ("..." if len(snippets) == max_snippets else "")

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search_es_pr', methods=['GET'])
def search_es_pr():
    start = time.time()
    query_term = request.args.get('query', '')
    print(query_term)

    # Elasticsearch query for PageRank-based search
    es_results = es_pr_indexer.es_client.search(
        index='simple',
        source_excludes=['url_lists'],
        size=100,
        query={
            "script_score": {
                "query": {"match": {"text": query_term}},
                "script": {"source": "_score * doc['pagerank'].value"}  # Using PageRank and score
            }
        }
    )

    # Process PageRank results and highlight the query term in the title
    es_results_list = [
        {
            'title': highlight_query(hit["_source"]['title'], query_term),
            'url': hit["_source"]['url'],
            'text': highlight_query(hit["_source"]['text'], query_term),
        }
        for hit in es_results['hits']['hits']
    ]

    # Elasticsearch query for TF-IDF-based search
    tfidf_results = tfidf_indexer.es_client.search(
        index='extend',  # Adjust this index if necessary
        source_excludes=['url_lists'],
        size=100,
        query={
            "match": {
                "text": query_term
            }
        }
    )

    # Process TF-IDF results
    tfidf_results_list = [
        {
            'title': highlight_query(hit["_source"]['title'], query_term),
            'url': hit["_source"]['url'],
            'text': highlight_query(hit["_source"]['text'], query_term),
        }
        for hit in tfidf_results['hits']['hits']
    ]

    # Combine both sets of results (Here we just concatenate them)
    combined_results = es_results_list + tfidf_results_list
    print(combined_results)
    # Sort the combined results by score, if desired
    combined_results = sorted(combined_results, key=lambda x: x.get('score', 0), reverse=True)

    # Get total hit count (You may want to count unique results)
    total_hit = len(combined_results)

    end = time.time()

    return render_template(
        'search_results.html', 
        query=query_term, 
        pr_results=es_results_list,  
        tfidf_results=tfidf_results_list, 
        total_hit=total_hit, 
        elapse=end - start
    )

if __name__ == '__main__':
    app.run(debug=True)
