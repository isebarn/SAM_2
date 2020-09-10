import scrapy
import pymongo
from bson.objectid import ObjectId
from pprint import pprint
import re
import os
from time import time
from scrapy import signals
from bs4 import BeautifulSoup
from difflib import HtmlDiff
import textwrap
from urllib.parse import urlparse, urlsplit
from datetime import datetime

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

def item_is_email_link(item):
  return 'mailto' in item

def starts_with_subdomain(url, parent_url):
  try:
    subdomain = re.search(r'([a-z0-9]+[.])*{}'.format(parent_url), url)

  except Exception as e:
    print("Problem with url: {}".format(url))
    return None

  return subdomain != None and subdomain.group(1) != None

def item_is_root(item, parent_url):
  # remove all prefixes
  clean_item = item.replace(parent_url, '').replace("www.", "").split("://")[-1].replace('/', '')
  if clean_item == '':
    return True

  return False

def item_is_outside_domain(item, parent_url):
  if '://' not in item: return False

  item_domain = urlsplit(item).netloc.split('.')[-2]

  parent = parent_url
  if '://' in parent:
    parent = urlsplit(parent).netloc
  parent_url_domain = parent.split('.')[-2]

  return item_domain is not None and item_domain != parent_url_domain

def filter_conditions(item, parent_url):
  if 'javascript:popup' in item \
    or item_is_empty(item) \
    or item_is_pdf_link(item) \
    or item_is_email_link(item) \
    or item_is_outside_domain(item, parent_url) \
    or item_is_root(item, parent_url):
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

  return "{}{}{}".format(parent_url, '' if item.startswith('/') else '/', item)


def save_single(data, collection_name):
  collection = get_mongo_collection(collection_name)
  collection_idx = levels.index(collection_name)

  collection.insert_one(data)

def save_many(data, collection_name):
  collection = get_mongo_collection(collection_name)
  collection_idx = levels.index(collection_name)

  pages = []

  for page in data:
    page['subpages'] = list(set(page['subpages']) - set(pages))
    pages += page['subpages']

  collection.insert_many(data)

def query_links(url, collection_name):
  collection = get_mongo_collection(collection_name)

  if collection_name == 'root':
    myquery = { "root": url }
    mydoc = collection.find_one(myquery)

    return mydoc['subpages']

  else:
    myquery = { "root": url }
    mydoc = collection.find(myquery)

    return [x['subpages'] for x in mydoc if len(x['subpages']) > 0]



def parser(response):
  url = response.request.url
  html_links = response.xpath("//div/a[@href]")
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

################# DEFINETALLY USED##############################

def client():
  return pymongo.MongoClient(os.environ.get('DATABASE'))

def get_mongo_collection(collection_name):
  database = client()["SAM2"]
  collection = database[collection_name]

  return collection

def get_root_item(root):
  root_collection = get_mongo_collection('root')
  query = { "root": root}
  root_item = root_collection.find_one(query)

  return root_item

def save_version(version_item, version):
  collection = get_mongo_collection(version)
  query = { "page_id": ObjectId(version_item['page_id']), "resolved": False}

  if collection.find(query).count() == 0:
    version_collection = get_mongo_collection(version)
    version_collection.insert_one(version_item)

  return version_item

def compare_html(original, new):
  soupA = [y.replace('\\n', '') for y in BeautifulSoup(original, features='lxml').stripped_strings if y != '\\n']
  soupB = [y.replace('\\n', '') for y in BeautifulSoup(new, features='lxml').stripped_strings if y != '\\n']

  c = HtmlDiff(wrapcolumn=50)
  diff_table = c.make_file(soupA, soupB, context=True).replace('\n', ' ')

  return diff_table

def get_version_number(_id):
  collection = get_mongo_collection('version')
  query = { "page_id":  _id}
  version_documents = collection.find(query)
  return len(list(version_documents)) + 1


################################################################

class Level2Spider(scrapy.Spider):
  name = "level2"

  def start_requests(self):
    start_urls = read_sites_file()
    level_2_collection = get_mongo_collection('level_2')

    for start_url in start_urls:
      self.root = get_root_item(start_url)
      query = { "root": start_url}
      self.level_2 = {str(x['_id']): x for x in level_2_collection.find(query)}
      keys = list(self.level_2.keys())

      # remove unresolved from the set of keys we scrape
      unresolved = get_unresolved_pages_levels([ObjectId(key) for key in keys], 'version_level_2')
      for x in unresolved:
        self.level_2.pop(str(x))

      keys = list(self.level_2.keys())

      for key in keys:

        try:
          yield scrapy.Request(url=fix_url(self.level_2[key]['url']),
            callback=self.parse,
            errback=self.errbacktest,
            meta={
              'root': self.level_2[key]['root'],
              '_id': str(key)
            })

        except Exception as e:
          pass

  def parse(self, response):
    try:
      result = response.text

    except AttributeError as e:
      result = ''

    except Exception as e:
      result = ''

    # if this is the first time this level_2 page was requested
    if self.level_2[response.meta.get('_id')]["body"] == '':
      self.level_2[response.meta.get('_id')]["body"] = result
      self.level_2[response.meta.get('_id')]["time"] = datetime.now()

    # not the first time this level_2 page was requested
    elif self.level_2[response.meta.get('_id')]["body"] != result:
      version_item = {}
      version_item["page_id"] = self.level_2[response.meta.get('_id')]["_id"]
      version_item["body"] = result
      version_item['diff'] = compare_html(self.level_2[response.meta.get('_id')]["body"], result)
      version_item['version_no'] = get_version_number(self.level_2[response.meta.get('_id')]["_id"])
      version_item['resolved'] = False
      version_item['time'] = datetime.now()
      save_version(version_item, 'version_level_2')

  def errbacktest(self, failiure):
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    level_2_collection = get_mongo_collection('level_2')
    bulk = level_2_collection.initialize_ordered_bulk_op()
    for _id, level in self.level_2.items():
      bulk.find({'_id': ObjectId(_id)}).update({'$set': {
        "body": level.get("body", ''),
        "time": level.get("time", '')}})

    if len(self.level_2.items()) > 0:
      bulk.execute()

class Level1Spider(scrapy.Spider):
  name = "level1"
  results = []
  unique_subpages = []

  def update_root(self, root_item):
    root_collection = get_mongo_collection('root')
    query = { "_id": ObjectId(root_item["_id"])}
    update = { "$set": { "subsubpages": root_item["subpages"], "body": root_item["body"] } }

    root_collection.update_one(query, update)

  def get_level_1_item(self, _id):
    level_1_collection = get_mongo_collection('level_1')
    query = { "_id": ObjectId(_id)}
    level_1_item = level_1_collection.find_one(query)

    return level_1_item

  def save_level_2(self, new_urls):
    if len(new_urls) == 0: return None

    level_2_collection = get_mongo_collection('level_2')
    new_ids = level_2_collection.insert_many(new_urls).inserted_ids

    return new_ids

  def start_requests(self):
    start_urls = read_sites_file()
    url_chunks = [{'root': url, 'urls': query_links(url, 'root')} for url in start_urls]

    level_1_collection = get_mongo_collection('level_1')

    for chunk in url_chunks:

      # get a list of urls that do NOT have unresolved versions
      ids = [x['_id'] for x in chunk['urls']]
      unresolved = get_unresolved_pages_levels(ids, 'version_level_1')
      urls = [url for url in chunk['urls'] if url['_id'] not in unresolved]

      # query the root item
      self.root = get_root_item(chunk['root'])

      # query the level 1 item
      query = { "root": chunk['root']}
      self.level_1 = {str(x['_id']): x for x in level_1_collection.find(query)}

      for url in urls:
        try:
          yield scrapy.Request(url=fix_url(url['url']),
            callback=self.parse,
            errback=self.errbacktest,
            meta={'root': chunk['root'], '_id': str(url['_id'])})

        except Exception as e:
          pass

  def parse(self, response):
    urls = response.xpath('//a[@href]/@href').extract()
    urls = list(set(urls))
    urls = [filter_conditions(x, response.meta.get('root')) for x in urls]
    urls = [x for x in urls if x is not None]
    urls = list(set(urls) - set([x["url"] for x in self.level_1[response.meta.get('_id')]["subpages"]]))
    urls = list(set(urls) - set([x["url"] for x in self.root["subpages"]]))
    urls = list(set(urls) - set([x["url"] for x in self.root["subsubpages"]]))
    urls = list(set(urls) - set(self.unique_subpages))

    ids = []

    # necessary for race conditions
    filtered_urls = []
    if len(urls) > 0:
      level_2_items = []
      # need a while loop to make sure that no two running threads
      # scraping the different subpages are collecting the same subsubpage url
      while len(urls) > 0:
        url = urls.pop()
        if url not in self.unique_subpages:
          self.unique_subpages.append(url)
          filtered_urls.append(url)
          level_2_item = {}
          level_2_item["root"] = response.meta.get('root')
          level_2_item["url"] = url
          level_2_item["parent"] = self.level_1[response.meta.get('_id')]['url']
          level_2_item["body"] = ''


          level_2_items.append(level_2_item)

      # save the subpages, and store the ids
      ids = self.save_level_2(level_2_items)

    # subpages will be added to the level_1 item
    if ids != None:
      subpages = [{'_id': _id, 'url': url.split("://www.")[-1].split("://")[-1]} for _id, url in zip(ids, filtered_urls)]
      self.level_1[response.meta.get('_id')]["subpages"].extend(subpages)

    # if this is the first time this level_1 page was requested
    if self.level_1[response.meta.get('_id')]["body"] == '':
      self.level_1[response.meta.get('_id')]["body"] = response.text
      self.level_1[response.meta.get('_id')]["time"] = datetime.now()

    # not the first time this level_1 page was requested
    elif self.level_1[response.meta.get('_id')]["body"] != response.text:
      version_item = {}
      version_item["page_id"] = self.level_1[response.meta.get('_id')]["_id"]
      version_item["body"] = response.text
      version_item['diff'] = compare_html(self.level_1[response.meta.get('_id')]["body"], response.text)
      version_item['version_no'] = get_version_number(self.level_1[response.meta.get('_id')]["_id"])
      version_item['resolved'] = False
      version_item['time'] = datetime.now()
      save_version(version_item, 'version_level_1')

    # subpages will also be added to the root item as subsubpages
    self.root["subsubpages"].extend(subpages)

  def errbacktest(self, failiure):
    print('fail')
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    level_1_collection = get_mongo_collection('level_1')
    bulk = level_1_collection.initialize_ordered_bulk_op()
    for _id, level in self.level_1.items():
      bulk.find({'_id': ObjectId(_id)}).update({'$set': {
        "subpages": level.get("subpages", []),
        "time": level.get("time", []),
        "body": level.get("body", '')}})

    if len(self.level_1.items()) > 0:
      bulk.execute()

    self.update_root(self.root)


class RootSpider(scrapy.Spider):
  name = "root"

  def save_root_basic(self, root_item):
    root_collection = get_mongo_collection('root')
    root_collection.insert_one(root_item)

    return root_item

  def save_level_1(self, new_urls):
    if len(new_urls) == 0: return None

    level_1_collection = get_mongo_collection('level_1')
    new_ids = level_1_collection.insert_many(new_urls).inserted_ids

    return new_ids

  def update_root(self, root_item):
    root_collection = get_mongo_collection('root')
    query = { "_id": ObjectId(root_item["_id"])}
    update = { "$set": { "subpages": root_item["subpages"], "body": root_item["body"] } }

    root_collection.update_one(query, update)

  def start_requests(self):
    start_urls = read_sites_file()
    unresolved_pages = get_unresolved_pages('root', start_urls)
    for url in start_urls:
      root_item = get_root_item(url)
      if root_item == None or root_item['_id'] not in unresolved_pages:
        yield scrapy.Request(url=fix_url(url), callback=self.parser, errback=self.errbacktest, meta={'root': url})

  def parser(self, response):
    urls = response.xpath('//a[@href]/@href').extract()
    urls = list(set(urls))
    urls = [filter_conditions(x, response.meta.get('root')) for x in urls]
    urls = [x for x in urls if x is not None]


    # check if root exists in collection
    root_item = get_root_item(response.meta.get('root'))
    # if not, save basic root
    if root_item == None:
      root_item = {}
      root_item["root"] = response.meta.get('root')
      root_item["url"] = response.url.split("://www.")[-1].split("://")[-1]
      root_item["subpages"] = []
      root_item["subsubpages"] = []
      root_item["body"] = response.text
      root_item['time'] = datetime.now()
      root_item = self.save_root_basic(root_item)

    # if new version, save version item
    elif root_item['body'] != response.text:
      version_item = {}
      version_item["page_id"] = root_item["_id"]
      version_item["body"] = response.text
      version_item['diff'] = compare_html(root_item['body'], response.text)
      version_item['version_no'] = get_version_number(root_item["_id"])
      version_item['resolved'] = False
      version_item['time'] = datetime.now()
      save_version(version_item, 'version_root')

    # iterate urls and save children as url + root
    urls = list(set(urls) - set([x["url"] for x in root_item["subpages"]]))
    level_1_items = []
    for url in urls:
      level_1_item = {}
      level_1_item["root"] = response.meta.get('root')
      level_1_item["url"] = url.split("://www.")[-1].split("://")[-1]
      level_1_item["subpages"] = []
      level_1_item["body"] = ''
      level_1_items.append(level_1_item)

    # save the subpages, and store the ids
    ids = self.save_level_1(level_1_items)

    # subpages will be added to the root item
    if ids != None:
      subpages = [{'_id': _id, 'url': url} for _id, url in zip(ids, urls)]
      root_item["subpages"].extend(subpages)

    # mark child urls with db ids and parent
    #
    # update the root with the new subpage list
    self.update_root(root_item)

  def errbacktest(self, failiure):
    pass


def get_unresolved_pages(collection_name, urls):
  collection = get_mongo_collection(collection_name)
  items = [item for item in collection.find({"root": {"$in": urls}})]
  ids = [item["_id"] for item in items]

  version_collection = get_mongo_collection("version_{}".format(collection_name))
  return [x['page_id'] for x in version_collection.find({"page_id": {"$in": ids}, 'resolved': False})]

def get_unresolved_pages_levels(page_ids, version):
  collection = get_mongo_collection(version)
  unresolved = collection.find({"page_id": {"$in": page_ids}, 'resolved': False})
  return [x['page_id'] for x in unresolved]


if __name__ == "__main__":
  pass