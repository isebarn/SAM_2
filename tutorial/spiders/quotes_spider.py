import scrapy
import pymongo
from pprint import pprint
import re
import os

levels = ['root', 'level_1', 'level_2', 'level_3']

# remove_prefix('House123', 'House') returns 123
def remove_prefix(text, prefix):
  if text.startswith(prefix):
      return text[len(prefix):]

  return text

# remove_all_prefixes('https://www.example.com') returns example.com
def remove_all_prefixes(item):
  url = item

  url = remove_prefix(url, 'https')
  url = remove_prefix(url, 'http')
  url = remove_prefix(url, '://')
  url = remove_prefix(url, 'www.')

  return url

def item_is_empty(item):
  return len(item) == 0

def item_is_pdf_link(item):
  return item.endswith('.pdf')

# item_is_subdirectory(/p/monday) return true
def item_is_subdirectory(item):
  return item.startswith('/')

def starts_with_subdomain(url, parent_url):
  try:
    subdomain = re.search(r'([a-z0-9]+[.])*{}'.format(parent_url), url)

  except Exception as e:
    print("Problem with url: {}".format(url))
    return None

  return subdomain != None and subdomain.group(1) != None

def filter_conditions(item, parent_url):
  if item_is_empty(item) or item_is_pdf_link(item):
    return None

  # Check if the parent_url appears in the item
  if parent_url in item:
    url = remove_all_prefixes(item)

    # check if the cleaned url starts with the parent url
    if url.startswith(parent_url):
      return item

    # check if url is a subdomain of the parent_url
    if starts_with_subdomain(url, parent_url):
      return item

  # check if item is a subdirectory link
  if item_is_subdirectory(item):
    return "{}{}".format(parent_url, item)

  return None

def client():
  return pymongo.MongoClient(os.environ.get('DATABASE'))

def get_mongo_collection(collection_name):
  database = client()["SAM2"]
  collection = database[collection_name]

  return collection

def save_single(data, collection_name):
  collection = get_mongo_collection(collection_name)
  collection_idx = levels.index(collection_name)

  collection.insert_one(data)

def save_many(data, collection_name):
  collection = get_mongo_collection(collection_name)
  collection_idx = levels.index(collection_name)

  collection.insert_many(data)

def query_links(url, collection_name):
  collection = get_mongo_collection(collection_name)

  if collection_name == 'root':
    myquery = { "root": url }
    mydoc = collection.find_one(myquery)

    return mydoc['subpages']

  else:
    result = []
    myquery = { "root": url }
    mydoc = collection.find(myquery)

    return [x['subpages'] for x in mydoc if len(x['subpages']) > 0]

def parser(response):
  url = response.request.url
  html_links = response.xpath("//a[@href]")
  links = [link.xpath('@href').extract_first() for link in html_links]
  links = list(set(links))
  links = [filter_conditions(link, url) for link in links]
  links = [link for link in links if link != None]

  result = {}
  result['url'] = url
  result['subpages'] = links
  result['root'] = response.meta.get('root')
  result['html'] = response.body.decode("utf-8")
  result['status'] = response.status

  return result

def read_sites_file():
    with open('sites.txt') as f:
        start_urls = [url.strip() for url in f.readlines()]

    return start_urls

def fix_url(url):
  if not url.startswith('http://') and not url.startswith('https://'):
    url = "http://www.{}".format(url)

  url = url[0:8] + url[8:].replace('//', '/')

  return url

class Level1Spider(scrapy.Spider):
  name = "level1"
  results = []

  def start_requests(self):
    start_urls = read_sites_file()
    url_chunks = [query_links(url, 'root') for url in start_urls]

    for chunk in url_chunks:
      for url in chunk:
        yield scrapy.Request(url=fix_url(url), callback=self.parse, errback=self.errbacktest, meta={'root': url})

      save_many(self.results, 'level_1')
      self.results = []


  def parse(self, response):
    result = parser(response)
    self.results.append(result)

  def errbacktest(self, failiure):
    response = failiure.value.response
    url = response.request.url

    result = {}
    result['url'] = url
    result['subpages'] = []
    result['root'] = response.meta.get('root')
    result['html'] = ''
    result['status'] = response.status

    self.results.append(result)


class RootSpider(scrapy.Spider):
  name = "root"

  def start_requests(self):
    start_urls = read_sites_file()

    for url in start_urls:
      yield scrapy.Request(url=fix_url(url), callback=self.parse, errback=self.errbacktest, meta={'root': url})

  def parse(self, response):
    result = parser(response)
    save_single(result, 'root')

  def errbacktest(self, failiure):
    response = failiure.value.response
    url = response.request.url

    result = {}
    result['url'] = url
    result['subpages'] = []
    result['root'] = response.meta.get('root')
    result['html'] = ''
    result['status'] = response.status

    save_single(result, 'root')

if __name__ == "__main__":
  pprint(query_links('ru.is', 'root'))