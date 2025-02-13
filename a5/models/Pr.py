from pathlib import Path
import os
import json
import pandas as pd
import numpy as np

class Pr:

    def __init__(self, alpha):
        self.crawled_folder = Path("resources/crawled")
        self.alpha = alpha

    def url_extractor(self):
        url_maps = {}
        all_urls = set([])

        for file in os.listdir(self.crawled_folder):
            if file.endswith(".txt"):
                j = json.load(open(os.path.join(self.crawled_folder, file)))
                all_urls.add(j['url'])
                for s in j['url_lists']:
                    all_urls.add(s)
                url_maps[j['url']] = list(set(j['url_lists']))
        all_urls = list(all_urls)
        return url_maps, all_urls
  
    def pr_calc(self):
        url_maps, all_urls = self.url_extractor()
        url_matrix = pd.DataFrame(columns=all_urls, index=all_urls)

        for url in url_maps:
            if len(url_maps[url]) > 0 and len(all_urls) > 0:
                url_matrix.loc[url] = (1 - self.alpha) * (1 / len(all_urls))
                url_matrix.loc[url, url_maps[url]] = url_matrix.loc[url, url_maps[url]] + (self.alpha * (1 / len(url_maps[url])))

        url_matrix.loc[url_matrix.isnull().all(axis=1), :] = (1 / len(all_urls))
        x0 = np.matrix([1 / len(all_urls)] * len(all_urls))
        P = np.asmatrix(url_matrix.values)

        prev_Px = x0
        Px = x0 * P
        i = 0
        while (any(abs(np.asarray(prev_Px).flatten() - np.asarray(Px).flatten()) > 1e-8)):
            i += 1
            prev_Px = Px
            Px = Px * P

        print('Converged in {0} iterations: {1}'.format(i, np.around(np.asarray(Px).flatten().astype(float), 5)))

        self.pr_result = pd.DataFrame(Px, columns=url_matrix.index, index=['score']).T