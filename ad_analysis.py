## Ad Research

from warcio.archiveiterator import ArchiveIterator # do not import nltk.corpus stopwords
from bs4 import BeautifulSoup
import extractwarc
import random
import csv

# runtime = 1:30 HR 

PATH='crawl-data/CC-MAIN-2017-13/segments/1490218186353.38/warc/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz'

def most_common_adwords(record, adwords, l_no, ifr_no, s_no):

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
    adwords = open_adwords()

    links = get_links(soup)
    iframes = get_iframes(soup)
    scripts = get_scripts(soup)

    adwords, l_no, ifr_no, s_no = has_ads(links, iframes, scripts, adwords, l_no, ifr_no, s_no)

    return adwords, l_no, ifr_no, s_no

def has_ads(links, iframes, scripts, adwords, l_no, ifr_no, s_no):

    for iframe in iframes:  
        for word in adwords:
            if word in iframe:
                adwords[word] += 1
                ifr_no += 1

    for link in links:  
        for word in adwords:
            if word in link:
                adwords[word] += 1
                l_no += 1

    for script in scripts:  
        for word in adwords:
            if word in script:
                adwords[word] += 1
                s_no += 1

    return adwords, l_no, ifr_no, s_no

def build_adwords_dict():
    adwords = {}
    with open('input/adwords.txt') as myfile:
        for line in myfile:
            adwords[line.strip()] = 0
    return adwords

def get_iframes(soup):
    links = []
    for link in soup.find_all('iframe'):
        links.append(link.get('src', ''))
    return links

def get_scripts(soup):
    links = []
    for link in soup.find_all('script'):
        links.append(link.get('src', ''))
    return links

def get_links(soup):
    return extractwarc.get_links(soup)

def is_english(title):
    return extractwarc.is_english(title)

def get_plaintext(soup):
    return extractwarc.get_plaintext(soup)

def open_stopwords():
    return extractwarc.open_stopwords()


if __name__ == '__main__':
    i = 0
    l_no = 0
    ifr_no = 0
    s_no = 0 

    adwords = build_adwords_dict()

    with open(PATH, 'rb') as stream:             
        for record in ArchiveIterator(stream):
            i += 1    # iterate over generator
            processed = most_common_adwords(record, adwords, l_no, ifr_no, s_no) 
            if processed:
                adwords, l_no, ifr_no, s_no = processed

        w = csv.writer(open('ad_analysis_results.csv', 'w'))
        for key, val in adwords.items():
            w.writerow([key, val])

        with open('ad_analysis_counts.txt', 'w') as f:
            f.write('list count: ' + '%d' % l_no + '\n')
            f.write('iframe count: ' + '%d' % ifr_no + '\n')
            f.write('script count: ' + '%d' % s_no)


