
# Project Goal

This project aims to build a search engine that only returns results from websites that do not contain advertisements. The crawl data is provided by Common Crawl, a non-profit that maintains an open repository of pedabytes of web archive data. 


# Processing Common Crawl Dataset

This project will process the Common Crawl dataset with Apache Spark and Python. Common Crawl stores its crawl data using the Web ARChive (WARC) format and variations of it:

+ WARC files which store the raw crawl data
+ WAT files which store computed metadata for the data stored in the WARC
+ WET files which store extracted plaintext from the data stored in the WARC

We will use the WAT files to assemble an inverted index by extracting keywords and to determine presence of advertisements from the metadata. 

1. Build inverted index. Run extractjob.py on local Spark cluster (which runs extractwarc.py), which processes WARC file, extracting keyword, title, description, url and keyword count within document. Returns parquet file.

2. Rank using tf-idf. Run tf_idf.py, which ranks query results by calculating term frequency - inverse document frequency using pandas and pyarrow. Returns updated parquet file.

3. Search engine GUI. Run search.py to initiate Flask instance.

# Running Spark cluster for batch processing

For running the scripts over larger chunks of data, we will spin up a Spark cluster using AWS Elastic MapReduce. The extracted keywords will be stored in parquet files on Amazon S3 buckets.





