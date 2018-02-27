## Script to process WARC files on local machine
## Extract keyword, title, url, etc to build inverted index

from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
# do not import nltk.corpus stopwords
from collections import Counter
import re
import time

# TODO: Dog and dog counted separatedly
# add title as keywords (heavy weight)
# check is_english without soup 
# can you pull out title, url, keywords using regex without bs4?
# TODO: WARNING:root:Some characters could not be decoded, and were replaced with REPLACEMENT CHARACTER.

# consider adding links & plaintext to final parquet files
# debut strategy: keep everything, note when something gets filtered
# calculate the cost on EMR

# TODO: page rank with tf-df
# TODO: run on Spark Cluster via EMR 

PATH='crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz'

def process_record(record):

	url = record.rec_headers['WARC-Target-URI']
	test_url = 'http://almosthomeohio.org/in-memory/leslies-cosette/'
	if url != test_url:
		return

	# skip WARC requests or metadata records
	if record.rec_type != 'response':
		return 
	content_type = record.http_headers.get_header('content-type', None)
	
	# skip non-HTML or unknown content types
	if content_type is None or 'html' not in content_type:
		return 

	soup = BeautifulSoup(record.content_stream().read(), 'html.parser')
	title = get_title(soup)

	# skip non-English records
	if not is_english(title):
		return 

	url = record.rec_headers['WARC-Target-URI']
	links = get_links(soup)
	description = get_description(soup)
	plaintext = get_plaintext(soup)

	adwords = open_adwords()
	stopwords = open_stopwords()

	if not has_ads(links, adwords):
		return 

	for word in top_words(plaintext, stopwords):
		yield unicode(url), unicode(title), unicode(description), unicode(word)

def is_english(title):
	# assume en if title cannot be ascii-decoded
	if title: # sometimes title is None
		try:       
			return title.decode('ascii')
		except UnicodeEncodeError: 
			return False

def get_title(soup):	
    return '' if soup.title is None else soup.title.string

def get_description(soup):
	for meta in soup.find_all('meta'):
		if meta.get('name', '').lower() == 'description':
			return meta.get('content', '')

def get_links(soup):
	links = []
	for link in soup.find_all('a'):
		links.append(link.get('href', ''))
	return links

def get_plaintext(soup):
	[tag.extract() for tag in soup(['style', 'script', 'a'])]
	return soup.get_text().replace('\n', ' ')

def open_adwords():
	adwords = []
	with open('adwords.txt') as myfile:
		for line in myfile:
			adwords.append(line.strip())
	return adwords

def has_ads(links, adwords):
	for link in links:  
		if any(x in link for x in adwords):
			return True

def open_stopwords():
	with open('stopwords.txt') as myfile:     
		return myfile.readlines()[0].split(',')

def get_words_iter(txt, stopwords):
	word = ''
	for letter in txt:
		if letter.isalnum():
			word += letter
		else:
			if word.lower() not in stopwords:
				yield word
			word = ''
			
def top_words(plaintext, stopwords):
	cleaned_txt = re.compile('[\W_]+').sub(' ', plaintext)
	words = get_words_iter(cleaned_txt, stopwords)
	return [w for w, count in Counter(words).most_common(10)]

i=0
with open(PATH, 'rb') as stream:             
	for record in ArchiveIterator(stream):   
		# print '#', i
		i+=1
		if i > 200:  
			break
		process_record(record)
		# print '---------'

