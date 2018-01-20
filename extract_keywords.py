
## MAPPER SCRIPT

from warcio.archiveiterator import ArchiveIterator
import ujson as json
from nltk.corpus import stopwords

PATH='/Users/lxu213/data/cc-pyspark-master/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz'
# PATH='/Users/lxu213/data/cc-pyspark-master/crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wet/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wet.gz'

def myReducer(url, keywords):
	return url + keywords 

def myMapper(record):
	''' Returns list of keywords given WAT file'''
	if is_wat_json_record(record):
		record = json.loads(record.content_stream().read())
		url = get_url(record)
		title = get_title(record)
		ascii_title = is_title_ascii(title)  # remove non-english

		# remove stop words 
		# remove words with special characters (needs work)
		if title and ascii_title:  
			title = ' '.join([word for word in title.split() \
				if word not in stopwords.words('english') \
				and word.isalnum()])
			return title.split()
	else:
		pass


def is_wat_json_record(record):
	''' Return true if WARC record is a WAT record'''
	return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

def get_url(record):
	try:
		return record['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
	except KeyError, e:       # missing in metadata
		return None

def get_title(record):
	try:
		return record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title']
	except KeyError, e:       # missing in metadata
		return None

def is_title_ascii(title):
	try: 
		return title.decode('ascii')
	except:
		return None


i=0
with open(PATH, 'rb') as stream:              # stream is now set as the python built-in method, open file
	for record in ArchiveIterator(stream):    # iterate over a stream of WARC records
		# print '#', i
		i+=1
		if i > 500:  
			break
		myMapper(record)

# import pdb; pdb.set_trace()


# Don't think I can get description and keywords out. 
# Metas is a list of unsorted dictionaries with keys: name, content 

    	







