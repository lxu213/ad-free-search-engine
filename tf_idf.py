## Ranks query results using tf-idf

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastparquet import write

PQPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/add_count/part-00000-6d70eeb9-5f8f-450b-ad70-f431c336e72d-c000.snappy.parquet'
OUTPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/'

warc = pq.read_table(PQPATH, nthreads=4).to_pandas()  

def calculate_tf_idf(warc):

    warc['tf-idf'] = pd.Series(0, index=warc.index)
    tot_doc = len(warc)/10       # total documents

    for i in range(len(warc)):
        docs_w_term = len(warc.loc[warc['keywords'] == warc['keywords'][i]])
        warc.loc[i,'tf-idf'] = warc['val'][i]['count'] * np.log(tot_doc/docs_w_term)
        
def save_parquet(result):

    write(OUTPATH + 'add_count/tf_idf.parquet', warc, compression='snappy')



if __name__ == '__main__':
    result = calculate_tf_idf(warc)
    save_parquet(result)