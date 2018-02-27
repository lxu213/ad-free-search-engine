## Extract Keyword myMapper script 
## For local testing off Spark cluster

from warcio.archiveiterator import ArchiveIterator
import ujson as json
from nltk.corpus import stopwords

stopwords = stopwords.words('english')

#TODO: read in .WARC file into extract_kw_wat.py and return true for is_wat and check if there's 
# read .html, or raw_stream. look at what properties the record has in warcio: vars(record) or dir(record)
# get .WARC file into beautiful soup. Maybe beautiful soup can read .WARC file
# maybe first use warcio and then put into beautiful soul 


PATH='crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/wat/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.wat.gz'

def myReducer(url, keywords):
	return url + keywords 
def myMapper(record):
	''' Returns list of keywords given WAT file'''
	if is_wat_json_record(record):
		record = json.loads(record.content_stream().read())
		url = get_url(record)
		title = get_title(record)
		links = get_links(record)

		if title and is_english(title) and not has_ads(links):
			title_words = [word.lower() for word in title.split() \
				if word not in stopwords and word.isalnum()]

			for word in title_words: 
				return url, word, get_description(record)

def get_description(record):
	try:
		metas = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Metas']
		for mdict in metas:
			if mdict.get('name','').lower() == 'description':
				return mdict['content']	
		return ''

	except KeyError, e:       # missing in metadata
		return ''

def has_ads(links):
	''' Takes links (list of dictionaries) and returns boolean for whether or not links contain 'adsense' '''
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

def is_english(title):
    # if i can't encode using ascii, then assume english
	try: 
		return title.decode('ascii')
	except UnicodeEncodeError:  
		return False

def is_wat_json_record(record):
	''' Return true if WARC record is a WAT record'''
	return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

def get_links(record):
	try:
		return record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
	except KeyError, e:       # missing in metadata
		return []

def get_url(record):
	try:
		return record['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
	except KeyError, e:       # missing in metadata
		return ''

def get_title(record):
	try:
		return record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head']['Title']
	except KeyError, e:       # missing in metadata
		return ''


i=0
with open(PATH, 'rb') as stream:              # stream is now set as the python built-in method, open file
	for record in ArchiveIterator(stream):    # iterate over a stream of WARC records
		print '#', i
		i+=1
		if i > 46:  
			break
		myMapper(record)
		print '---------'
