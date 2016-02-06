
import threading
import re
import csv
import urlparse
import urllib2
import time
from datetime import datetime, date
import robotparser
import Queue
import pandas as pd


SLEEP_TIME = 1

def link_crawler(urls, link_regex=None, delay=1, max_depth=-1, max_urls=-1, headers=None, user_agent='ice-ix', proxy=None, num_retries=1, max_threads=10, scrape_callback=None):



    words_seen = 0

    def worker():


        while True:

            try:
                seed_url = urls.pop()
            except IndexError:
                # crawl queue is empty
                break
            else:

                """Crawl from the given seed URL following links matched by link_regex
                """

                # the queue of URL's that still need to be crawled
                crawl_queue = [seed_url]
                # the URL's that have been seen and at what depth
                seen = {seed_url: 0}
                # track how many URL's have been downloaded
                num_urls = 0
                df_sitewords = pd.DataFrame()
                words_found=0



                rp = get_robots(seed_url)
                throttle = Throttle(delay)
                # headers = headers or {}
                headers = {}
                if user_agent:
                    headers['User-agent'] = user_agent



                while crawl_queue:
                    url = crawl_queue.pop()
                    depth = seen[url]
                    # check url passes robots.txt restrictions
                    if rp.can_fetch(user_agent, url):
                        throttle.wait(url)
                        html = download(url, headers, proxy=proxy, num_retries=num_retries)
                        links = []
                        if scrape_callback:
                            # links.extend(scrape_callback(url, html) or [])
                            wst = scrape_callback(url, html)
                            wst['url'] = seed_url
                            wst['date'] = date.today().isoformat()
                            df_sub = pd.DataFrame.from_dict([wst])
                            df_sub = df_sub.set_index(['date','url'])
                            if df_sitewords.empty:
                                df_sitewords = df_sub
                            else:
                                df_sitewords = df_sitewords.add(df_sub)
                            words_found = words_found + wst['totalwords']
                        if depth != max_depth:
                            # can still crawl further
                            if link_regex:
                                # filter for links matching our regular expression
                                links.extend(link for link in get_links(html) if re.match(link_regex, link))

                            for link in links:
                                link = normalize(seed_url, link)
                                # check whether already crawled this link
                                if link not in seen:
                                    seen[link] = depth + 1
                                    # check link is within same domain
                                    if same_domain(seed_url, link):
                                        # success! add this new link to queue
                                        crawl_queue.append(link)

                        # check whether have reached downloaded maximum
                        num_urls += 1
                        if num_urls == max_urls:
                            break
                    else:
                        print 'Blocked by robots.txt:', url
                return_que.put([seed_url, words_found, df_sitewords])


    # multi-thread download and scraping work
    threads = []
    return_que = Queue.Queue()

    while threads or urls:
        # the crawl is still active
        for thread in threads:
            if not thread.is_alive():
                # remove the stopped threads
                threads.remove(thread)
        while len(threads) < max_threads and urls:
            # can start some more threads
            thread = threading.Thread(target=worker)
            thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)


        # all threads have been processed
        # sleep temporarily so CPU can focus execution on other threads
        time.sleep(SLEEP_TIME)

    df_results = pd.DataFrame()
    while not return_que.empty():
        url_words = return_que.get()
        # print(url_words[2])
        words_seen = words_seen + url_words[1]
        if df_results.empty:
            df_results = url_words[2]
        else:
            df_results = df_results.append(url_words[2])

    return df_results, words_seen


class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urlparse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()



def download(url, headers, proxy, num_retries, data=None):
    print 'Downloading:', url
    request = urllib2.Request(url, data, headers)
    opener = urllib2.build_opener()
    if proxy:
        proxy_params = {urlparse.urlparse(url).scheme: proxy}
        opener.add_handler(urllib2.ProxyHandler(proxy_params))
    try:
        response = opener.open(request, timeout=60)
        html = response.read()
        code = response.code
    except urllib2.URLError as e:
        print 'Download error:', e.reason
        html = ''
        if hasattr(e, 'code'):
            code = e.code
            if num_retries > 0 and 500 <= code < 600:
                # retry 5XX HTTP errors
                html = download(url, headers, proxy, num_retries-1, data)
        else:
            code = None
    return html


def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
    """
    link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
    return urlparse.urljoin(seed_url, link)


def same_domain(url1, url2):
    """Return True if both URL's belong to same domain
    """
    return urlparse.urlparse(url1).netloc == urlparse.urlparse(url2).netloc


def get_robots(url):
    """Initialize robots parser for this domain
    """
    rp = robotparser.RobotFileParser()
    rp.set_url(urlparse.urljoin(url, '/robots.txt'))
    rp.read()
    return rp


def get_links(html):
    """Return a list of links from html
    """
    # a regular expression to extract all links from the webpage
    webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
    # list of all links from the webpage
    return webpage_regex.findall(html)


def scrape_callback(url, html):

    negwords = ['anxiety', 'debt', 'recession', 'depress', 'slowdown', 'stagnant', 'decline', 'downturn', 'downtrend', 'loan',
                'deficit', 'collapse', 'deflation', 'inflation', 'slump', 'trouble', 'tumble', 'strike',
                'sluggish', 'weak', 'plunge', 'shortfall', 'fear', 'doubt', 'panic', 'jitters', 'worry',
                'terror','concern', 'bankrupt', 'insolvent', 'uncertain', 'crash' ]
    i = 0
    word_subtotals = {}
    for word in negwords:
        wcount = html.count(word)
        i = i + wcount
        wordlabel = 'word-' + word
        word_subtotals[wordlabel] = wcount

    word_subtotals['totalwords'] = i
    print url, i
    print word_subtotals
    # return i
    return word_subtotals



if __name__ == '__main__':

    # urls = alexa(30)

    urls = []
    with open('topnews.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            urls.append('http://' + row[0])

        print urls



    df_results, total_words = link_crawler(urls, '/*', max_depth=1, max_urls=20, scrape_callback=scrape_callback)
    # print df_results
    dt = date.today().strftime('%Y-%m-%d')
    outfilename = dt + 'output.csv'
    df_results.to_csv(outfilename)

    print('Total words: ')
    print total_words

    '''
       Improvement ideas:
        -- collect negative words by parent site and by word, store in rdbms and collect SPX, GOLD values for later analytics
        -- only scrape from HTML Body using better regex matching
        -- scrape positive words to measure positive sentiment
        -- pass in word inputs via config file
        -- add number of pages scanned on website


    '''