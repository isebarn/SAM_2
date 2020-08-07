# Scraper

## Setting up the environment

#### Virtualenv
I use virtualenv but it is not strictly speaking necessary

```
virtualenv venv
source /venv/bin/activate
```

#### Environment variables
You will need two environment variables, ```DATABASE``` and ```BROWSER```, for a database link and the address of the Selenium browser (optional)

```
export DATABASE=mongodb://root:example@192.168.1.35:27017/ <--- mine
export DATABASE=mongodb://user:password@address:port/
```
#### Python packages installation
```
pip3 install -r requirements.txt
or
pip install -r requirements.txt
```

# Usage

You must create a file called sites.txt, which should be line separated

### Scrape root

```
scrapy crawl root
```

### Scrape level 1

```
scrapy crawl level1
```