## Spark job that runs on local cluster 
## Inherits run_job funcs from CCSparkJob and build_index funcs from build_index.py

from sparkcc import CCSparkJob
from pyspark.sql.types import StructType, StructField, StringType, LongType
import ujson as json
from bs4 import BeautifulSoup
from collections import Counter
import re
import build_index

class BuildIndexJob(CCSparkJob):
    """ Extract keywords from Common Crawl WARC files """

    name = "BuildIndex"
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
        output = input_data.mapPartitionsWithIndex(self.build_index) 

        # create SQL DF from output RDD 
        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format("parquet") \
            .saveAsTable(self.args.output)

        self.get_logger(sc).info('records processed = {}'.format(
            self.records_processed.value))

    def build_index(self, record):
        return tuple(build_index.process_record(record))

    def is_english(self, title):
        return build_index.is_english(title)

    def get_title(self, soup):    
        return build_index.get_title(soup)

    def get_description(self, soup):
        return build_index.get_description(soup)

    def get_links(self, soup):
        return build_index.get_links(soup)

    def get_plaintext(self, soup):
        return build_index.get_plaintext(soup)

    def open_adwords(self):
        return build_index.open_adwords()

    def has_ads(self, links, adwords):
        return build_index.has_ads(links, adwords)

    def open_stopwords(self):
        return build_index.open_stopwords()

    def get_words_iter(self, txt, stopwords):
        return build_index.get_words_iter(txt, stopwords)

    def top_words(self, plaintext, stopwords):
        return build_index.top_words(plaintext, stopwords)


if __name__ == '__main__':
    job = BuildIndexJob()
    job.run()

