from sparkcc import CCSparkJob
from pyspark.sql.types import StructType, StructField, StringType, LongType
import ujson as json

class ExtractKeywordJob(CCSparkJob):
    """ Extract keywords from title in Common Crawl WAT files """

    name = "ExtractKeyword"

    output_schema = StructType([
        StructField("url", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        ])

    def process_record(self, record):
		''' returns list of keywords given WAT file'''
		if self.is_wat_json_record(record):
			record = json.loads(record.content_stream().read())
			url = self.get_url(record)
			title = self.get_title(record)
			links = self.get_links(record)

			if title and self.is_english(title) and not self.has_ads(links):
				descrip = self.get_description(record)

				title_words = [
					word for word in title.lower().split() \
					if not word.isdigit() and 
					not self.is_stopword(word) and
					word.isalnum()
				]
				for word in title_words: 
					yield url, word, title, descrip

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

    def is_wat_json_record(self, record):
		''' Return true if WARC record is a WAT record'''
		return (record.rec_type == 'metadata' and
	                record.content_type == 'application/json')

    def get_url(self, record):
    	# not using safe access .get on dict since so many key-value pairs could be missing
		try:
			return record['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
		except KeyError, e:       # missing in metadata
			return ''

    def get_title(self, record):
		try:
			return record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title']
		except KeyError, e:       # missing in metadata
			return ''

    def get_description(self, record):
		try:
			metas = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Metas']
			for mdict in metas:
				if mdict.get('name','').lower() == 'description':
					return mdict['content']	
			return ''

		except KeyError, e:       # missing in metadata
			return ''

    def get_headline(self, record):
		try:
			metas = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Metas']
			for mdict in metas:
				if mdict.get('name','').lower() == 'headline':
					return mdict['content']	
			return ''

		except KeyError, e:       # missing in metadata
			return ''

    def get_page_keywords(self, record):
		try:
			metas = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Metas']
			for mdict in metas:
				if mdict.get('name','').lower() == 'keywords':
					return mdict['content']	
			return ''

		except KeyError, e:       # missing in metadata
			return ''

    def get_section(self, record):
		try:
			metas = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Metas']
			for mdict in metas:
				if mdict.get('name','').lower() == 'section':
					return mdict['content']	
			return ''

		except KeyError, e:       # missing in metadata
			return ''

    def is_english(self, title):
	    # if title cannot be ascii-encoded, then assume english
		try: 
			return title.decode('ascii')
		except UnicodeEncodeError:  
			return False

    def get_links(self, record):
		try:
			return record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
		except KeyError, e:       # missing in metadata
			return []

    def is_stopword(self, word):
		stopwords_path = 'stopwords.txt'
		with open(stopwords_path, 'r') as myfile:     
			stopwords = myfile.readlines()[0].split(',')
		return word in stopwords

    def has_ads(self, links):
		''' Takes links (list of dictionaries)
		Returns boolean for whether or not links contain adwords'''
		adwords = []
		with open('adwords.txt') as myfile:     
			for line in myfile:
				adwords.append(line.strip())

		for d in links:  
			if d.get('url', None):   # .get can return Nonetype,
									 # which has no attribute "encode"
				url = d.get('url', u'').encode('ascii','ignore').lower()
				if any(x in url for x in adwords):  
					return True


if __name__ == '__main__':
    job = ExtractKeywordJob()
    job.run()


