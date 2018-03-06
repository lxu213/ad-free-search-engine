## Ranks query results using tf-idf

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastparquet import write
import time

# runtime O(n) on 1 parquet = 30 min
# TODO: how to run this at scale (parallelizing or sampling)

PQPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/has_rank/part-00000-5ede3ccf-f2ae-4379-865c-a660d10c03b4-c000.snappy.parquet'
OUTPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/'

warc = pq.read_table(PQPATH, nthreads=4).to_pandas()

def build_idf_dict(warc):

    tot_doc = len(warc)/10       # total documents
    idf_dict = {}

    for term in set(warc['keywords']):
        idf_dict[term] = np.log(tot_doc/len(warc.loc[warc['keywords'] == term]))

    return idf_dict

def calculate_tf_idf(warc, idf_dict):

    warc['tf-idf'] = pd.Series(0, index=warc.index)

    for idx, keyword in enumerate(warc['keywords']):
        warc.loc[idx,'tf-idf'] = warc['val'][idx]['count'] * idf_dict.get(keyword, 1.0)
        
    return warc
 
def save_parquet(result):

    write(OUTPATH + 'has_rank/testing_optimization.parquet', warc, compression='snappy')



if __name__ == '__main__':

    idf_dict = build_idf_dict(warc)
    result = calculate_tf_idf(warc, idf_dict)
    save_parquet(result)


