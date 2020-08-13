# wikiCategoryScraper.py
# Provides functions for getting all the subcategories and pages
# from a wikipedia category page
#
# Author: Bryan Ingwersen
# Date: April 10, 2020

import requests
from bs4 import BeautifulSoup

def convertToFullURL(base, url):
    '''convert a url to its full form from a the site name
    and the site sub-directory'''

    if url[0] == '/':
        return base + url
    else:
        return url

def scrapeWikiCategory(url, name=None):
    '''scrape all of the article and subcategory name from
    a wikipedia Category page'''

    pageDict = {} # dictionary of article titles to urls
    categories = {} # dictionary of category names to urls

    # retrieve the category page's html
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    
    # extract the divs with the subcategory and page lists
    subcategorySoup = soup.find('div', id='mw-subcategories')
    pageSoup = soup.find('div', id='mw-pages')
    
    # pull out all of the subcategory links
    subcategories = []
    if subcategorySoup is not None:
        subcategories = subcategorySoup.find_all('a', href=True)
    
    # pull out all the article links
    pages = []
    if pageSoup is not None:
        pages = pageSoup.find_all('a', href=True)

    # add the pages to the page dictionary (removing repeats)
    for page in pages:
        href = page['href']
        pageDict[page.text] = href
    
    # add all the subcategories and remove duplicates
    for category in subcategories:
        url = convertToFullURL('https://en.wikipedia.org', category['href'])
        name = category.text
        categories[name] = url

    return pageDict, categories        
