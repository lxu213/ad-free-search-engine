## Spark job that runs on local cluster 
## Inherits run_job funcs from CCSparkJob and process_record funcs from extractwarc.py

from sparkcc import CCSparkJob
from pyspark.sql.types import StructType, StructField, StringType, LongType
import ujson as json
from bs4 import BeautifulSoup
from collections import Counter
import re
import extractwarc

class ExtractKeywordJob(CCSparkJob):
    """ Extract keywords from Common Crawl WARC files """

    name = "ExtractKeyword"
    output_schema = StructType([
        StructField("keywords", StringType(), True),
        StructField("val", StructType([
            StructField("url", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("count", LongType(), True)]), True)
        ])
    
    def run_job(self, sc, sqlc):
        # convert .gz input file to RDD of strings
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)
        
        # map func process_warcs across partition while keeping index
        output = input_data.mapPartitionsWithIndex(self.process_warcs) 

        # create SQL DF from output RDD 
        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format("parquet") \
            .saveAsTable(self.args.output)

        self.get_logger(sc).info('records processed = {}'.format(
            self.records_processed.value))

    def process_record(self, record):
        return tuple(extractwarc.process_record(record))

    def is_english(self, title):
        return extractwarc.is_english(title)

    def get_title(self, soup):    
        return extractwarc.get_title(soup)

    def get_description(self, soup):
        return extractwarc.get_description(soup)

    def get_links(self, soup):
        return extractwarc.get_links(soup)

    def get_plaintext(self, soup):
        return extractwarc.get_plaintext(soup)

    def open_adwords(self):
        return extractwarc.open_adwords()

    def has_ads(self, links, adwords):
        return extractwarc.has_ads(links, adwords)

    def open_stopwords(self):
        return extractwarc.open_stopwords()

    def get_words_iter(self, txt, stopwords):
        return extractwarc.get_words_iter(txt, stopwords)

    def top_words(self, plaintext, stopwords):
        return extractwarc.top_words(plaintext, stopwords)

    # def reduce_by_key_func_temp(a, b):
    #     aurl, atitle, adesc = a
    #     burl, btitle, bdesc = b
    #     return aurl, atitle, adesc



if __name__ == '__main__':
    job = ExtractKeywordJob()
    job.run()

