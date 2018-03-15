## Build inverted index by processing WARC files and saving into parquet files
## Extract keyword, title, url, description, count 

from warcio.archiveiterator import ArchiveIterator # do not import nltk.corpus stopwords
from bs4 import BeautifulSoup
from collections import Counter
import re                         

# runtime for 1 parq = 1 hour
# TODO: run on Spark
# TODO: upload demo

PATH='crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz'

def process_record(record):

    # skip WARC requests or metadata records
    if record.rec_type != 'response': 
        return 
    content_type = record.http_headers.get_header('content-type', None)

    # skip non-HTML or unknown content types
    if content_type is None or 'html' not in content_type:
        return 
    html = record.content_stream().read()

     # skip non-English records 
    title = re.search('<title>(.*?)</title>', html)
    if not title:
        return
    title = title.group()[7:-8]
    if not is_english(title):
        return 

    soup = BeautifulSoup(html, 'html.parser')
    links = get_links(soup)
    adwords = open_adwords()
    
    if has_ads(links, adwords):
        return 

    stopwords = open_stopwords()
    plaintext = get_plaintext(soup)
    description = get_description(soup)
    url = record.rec_headers['WARC-Target-URI']

    for word, count in top_words(plaintext, stopwords):
        yield unicode(word), (unicode(url), unicode(title), unicode(description), count)

def is_english(title):
    # assume en if title cannot be ascii-decoded
    if title: # sometimes title is None
        try:       
            return title.decode('ascii')
        except UnicodeDecodeError: 
            return False

def get_description(soup):
    for meta in soup.find_all('meta'):
        if meta.get('name', '').lower() == 'description':
            return meta.get('content', '')

def get_links(soup):
    links = []
    for link in soup.find_all('iframe'):
        links.append(link.get('src', ''))
    return links

def get_plaintext(soup):
    [tag.extract() for tag in soup(['style', 'script', 'a'])]
    return soup.get_text().replace('\n', ' ')

def open_adwords():
    adwords = []
    with open('input/adwords.txt') as myfile:
        for line in myfile:
            adwords.append(line.strip())
    return adwords

def has_ads(links, adwords):
    for link in links:  
        if any(x in link for x in adwords):
            return True

def open_stopwords():
    with open('input/stopwords.txt') as myfile:     
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
    words = get_words_iter(cleaned_txt.lower(), stopwords)
    return Counter(words).most_common(10)


if __name__ == '__main__':
    i = 0
    with open(PATH, 'rb') as stream:             
        for record in ArchiveIterator(stream):
            i += 1    # iterate over generator 
            [x for x in process_record(record)]    

