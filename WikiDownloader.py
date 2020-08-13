# wikiDownloader.py
# Download a compressed snapshot of the source text files for all
# articles in Engligh Wikipedia
#
# Author: Bryan Ingwersen
# Date: March 27, 2020

import requests
from pathlib import Path
from bs4 import BeautifulSoup

# the date of the dump in the form YYYYMMDD
# see https://dumps.wikimedia.org/enwiki/ for available dumps
dateString = '20200301'

# directory to save files to relative to current directory
downloadPath = Path('data'/'wikiDump')

# page with links to dump files
url = 'https://dumps.wikimedia.org/enwiki/{}/'.format(dateString)
html = requests.get(url).text
soup = BeautifulSoup(html, 'lxml')

# url that has all data files have in common
commonUrlText = 'enwiki-{}-pages-articles-multistream'.format(dateString)

# get all of the links and filter the ones that should be downloaded
fileLinks = []
aTags = soup.find_all('a', href=True)
for aTag in aTags:
    if (
        (commonUrlText in aTag.text) and
        (commonUrlText + '.' not in aTag.text) # ignore the combined file with all streams put together
    ):
        fileLinks.append(aTag['href'])

# download each link (this is about 16gb, it will take a while)
import urllib
for i, fileLink in enumerate(fileLinks):
    print("Retrieving File {}/{}: {}".format(i + 1, len(fileLinks), 'https://dumps.wikimedia.org' + fileLink))
    urllib.request.urlretrieve('https://dumps.wikimedia.org' + fileLink, downloadPath / fileLink[17:])
    print("Done retrieving File!")