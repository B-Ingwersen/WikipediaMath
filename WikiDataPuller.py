# wikiDataPuller.py
# This program provides abstracted ways of accessing article
# metadata and content contained in the wiki data dump
#
# Author: Bryan Ingwersen
# Date: April 4, 2020

import os
import json
import bz2
import math
from pathlib import Path
from bs4 import BeautifulSoup
import numpy as np
import requests
import WikiCategoryScraper
from ThreadPool import ThreadPool
from datetime import datetime

# the directory in which the dump data is stored
DATA_PATH = Path('data/wikiDump')

class DataPuller:
    '''a standard set of functions for loading article information
    from the wiki dump data'''
    
    # file descriptors for index files
    blockListFile = None
    mainListFile = None
    idListFile = None
    idHashFile = None
    titleListFile = None
    titleHashFile = None
    HASH_MAP_SIZE = 6000000

    fileIndex = None

    def __init__(self):
        # load all the index files if they are not already loaded
        try:
            INDEX_PATH = DATA_PATH / 'MainIndex'

            if DataPuller.blockListFile is None:
                DataPuller.blockListFile = open(INDEX_PATH / 'blockList.bin', 'rb')
            if DataPuller.mainListFile is None:
                DataPuller.mainListFile = open(INDEX_PATH / 'mainList.bin', 'rb')
            if DataPuller.idListFile is None:
                DataPuller.idListFile = open(INDEX_PATH / 'idList.bin', 'rb')
            if DataPuller.idHashFile is None:
                DataPuller.idHashFile = open(INDEX_PATH / 'idHash.bin', 'rb')
            if DataPuller.titleListFile is None:
                DataPuller.titleListFile = open(INDEX_PATH / 'titleList.bin', 'rb')
            if DataPuller.titleHashFile is None:
                DataPuller.titleHashFile = open(INDEX_PATH / 'titleHash.bin', 'rb')
            if DataPuller.fileIndex is None:
                with open(DATA_PATH / 'fileIndex.json') as file:
                    DataPuller.fileIndex = json.load(fp=file)
        
        # if the index files are not found, abort the program now
        except:
            print('Main index data not found - run wikiDownloader.py and/or wikiDataIndexer.py')
            raise FileNotFoundError
    
    def uppercaseTitle(self, title):
        '''make uppercase the first letter of a title so that will match
        what is recorded in the index files'''

        if title is None or len(title) == 0:
            return title
        
        return title[0].upper() + title[1:]
    
    def stringHashFunc(self, s):
        '''
        hash a string using a polynomial rolling hash function
        source: https://cp-algorithms.com/string/string-hashing.html
        '''

        hash_ = 0
        p = 1
        for c in s:
            hash_ += ord(c) * p

            p *= 53
            # keep p well within 64 bit integer bounds
            # (prevent C-Python from switching to big integer internal format)
            p &= 0x0003FFFFFFFFFFFF
        
        # perform a modulo by a large prime number to restrict size of hash
        return hash_ % 982451653
    
    def getHashBucketNames(self, offset):
        '''load all the items in a bucket at location offset
        from the title list file'''

        # go to offset in the title list file
        DataPuller.titleListFile.seek(offset)

        entries = []
        while True:
            entryName = DataPuller.titleListFile.read(1)

            # break when the next title string is empty
            if entryName == b'\x00':
                break

            # read in the title string until reaching a null-terminator
            while True:
                c = DataPuller.titleListFile.read(1)
                if c == b'\x00':
                    break
                else:
                    entryName += c
            
            # add the title and the article number to the entries
            entryName = entryName.decode('utf-8')
            entryNum = int.from_bytes(DataPuller.titleListFile.read(4), 'little')
            entries.append([entryName, entryNum])
        
        return entries
    
    def getNumFromTitle(self, name):
        '''returning an article's number from its title'''

        # hash the title name
        name = self.uppercaseTitle(name)
        hash_ = self.stringHashFunc(name) % DataPuller.HASH_MAP_SIZE

        # get the offset of bucket in the title list
        DataPuller.titleHashFile.seek(4 * hash_)
        offset = int.from_bytes(DataPuller.titleHashFile.read(4), 'little')

        # return None if the bucket is empty
        if offset == 0xFFFFFFFF:
            return None

        bucketEntries = self.getHashBucketNames(offset)

        # return the num if the title is in the bucket
        for entry in bucketEntries:
            if entry[0] == name:
                return entry[1]
        return None
    
    def getInfoFromNum(self, num):
        '''return an article's mainList information based on its article number'''

        if num is None:
            return None
        
        DataPuller.mainListFile.seek(12 * num)
        id_ = int.from_bytes(DataPuller.mainListFile.read(4), 'little')
        titleOffset = int.from_bytes(DataPuller.mainListFile.read(4), 'little')
        block = int.from_bytes(DataPuller.mainListFile.read(4), 'little')

        return id_, titleOffset, block
    
    def getBlockInfo(self, blockIndex):
        '''get the location of a block given its index'''

        DataPuller.blockListFile.seek(12 * blockIndex)

        catalogIndex = int.from_bytes(DataPuller.blockListFile.read(4), 'little')
        start = int.from_bytes(DataPuller.blockListFile.read(4), 'little')
        end = int.from_bytes(DataPuller.blockListFile.read(4), 'little')

        return catalogIndex, start, end

    def getBlockText(self, blockIndex):
        '''load and decompress all the text in a block'''
        
        blockInfo = self.getBlockInfo(blockIndex)

        catalogIndex = blockInfo[0]
        start = blockInfo[1]
        length = blockInfo[2] - blockInfo[1]
        fileName = DataPuller.fileIndex['catalog'][catalogIndex]['xmlFilename']
        
        compressedData = None
        with open(DATA_PATH / fileName, 'rb') as file:
            file.seek(start)
            compressedData = file.read(length)
        
        return bz2.decompress(compressedData).decode('utf-8')
    
    def getTitleAtOffset(self, offset):
        '''get an article's title given its offset in the title list file'''
        DataPuller.titleListFile.seek(offset)

        entryName = DataPuller.titleListFile.read(1)
        # return None if the string is empty
        if entryName == b'\x00':
            return None
        while True:
            # read a string until a null terminator is reached
            c = DataPuller.titleListFile.read(1)
            if c == b'\x00':
                return entryName.decode('utf-8')
            else:
                entryName += c

    def getTextFromNum(self, num, title=None):
        '''get an article's text given it's number'''

        info = self.getInfoFromNum(num)

        # load the title if it was not passed to the function
        if title is None:
            title = self.getTitleAtOffset(info[1])
        title = self.uppercaseTitle(title)
        blockIndex = info[2]
        
        blockText = self.getBlockText(blockIndex)

        # serach for the title and the start and end tags of the article
        titleTagLoc = blockText.find('<title>' + title + '</title>')
        pageTagLoc = blockText.rfind('<page>', 0, titleTagLoc)
        endPageTagLoc = blockText.find('</page>', pageTagLoc) + len('</page>')

        articleText = blockText[pageTagLoc:endPageTagLoc]
        return articleText
    
    def getTextFromTitle(self, title):
        '''get the text of an article given its title'''

        num = self.getNumFromTitle(title)
        return self.getTextFromNum(num, title=title)

class ArticleGetter:
    '''A streamlined interface for getting information about an article'''

    def __init__(self, title=None, num=None):
        '''get article by it's title or article number'''

        # the DataPuller object that operations are made through
        self.dataPuller = DataPuller()

        self.exists = True
        self.text = None
        self.links = None
        self.linkNums = None

        if num is not None: # try loading by article number
            self.num = num

            # check that the article exists
            info = self.dataPuller.getInfoFromNum(num)
            if info is None:
                self.exists = False
                return
            
            # get the article's wikiID and title
            self.id_ = info[0]
            self.title = self.dataPuller.getTitleAtOffset(info[1])
        elif title is not None: # try loading article by title
            self.title = title

            # get the article's number
            self.num = self.dataPuller.getNumFromTitle(title)

            # check that the article exists
            info = self.dataPuller.getInfoFromNum(self.num)
            if info is None:
                self.exists = False
                return
            
            # save the article's wikiDI
            self.id_ = info[0]
        else:
            self.exists = False
    
    def getText(self):
        '''retrieve the text of an article'''

        if self.text is not None: # check if text already loaded
            return self.text
        
        if not self.exists:
            return None
        
        self.text = self.dataPuller.getTextFromNum(self.num, title=self.title)
        return self.text
    
    def getLinks(self):
        '''return the titles of all the links in the article's text'''

        if self.links is not None: # check if the links are already loaded
            return self.links

        text = self.getText()
        
        if text is None:
            return None
        
        linkLoc = 0
        links = []
        # search for wikitext links
        while True:
            linkLoc = text.find('[[', linkLoc)
            if linkLoc == -1:
                break
            linkLoc += 2
        
            endLoc = text.find(']]', linkLoc)
            sepLoc = text.find('|', linkLoc, endLoc)
            if sepLoc != -1:
                endLoc = sepLoc
            
            links.append(text[linkLoc:endLoc])
        
        self.links = links
        return self.links
    
    def getLinkNums(self):
        '''return the article numbers of all the links in the article's text'''

        # check if the link numbers are already loaded
        if self.linkNums is not None:
            return self.linkNums

        # get all the link titles
        links = self.getLinks()
        if links is None:
            return None
        
        # convert all the titles to article numbers
        linkNums = []
        for link in links:
            num = self.dataPuller.getNumFromTitle(link)
            if num is not None:
                linkNums.append(num)
        
        self.linkNums = linkNums
        return linkNums

    def getEditHistory(self):
        '''retrieve an article's revision history for the last 5 years
        from the wikimedia api'''

        editData = [] # store list of edits
        try:
            url = 'https://en.wikipedia.org/w/api.php' # api url

            # paramters to load first 100 edits
            params = {
                'action' : 'query',
                'prop' : 'revisions',
                'titles' : self.title,
                'format' : 'json',
                'rvlimit' : 100,
                'rvprop' : 'ids|timestamp|size|user'
            }

            headers = {
                'User-Agent' : 'EG10112 Math Wikipedia Articles Analysis (bryaningwersen@outlook.com)'
            }

            done = False

            while not done:
                text = requests.get(url, params=params, headers=headers).text
                data = json.loads(text)

                if not 'continue' in data:
                    done = True
                else:
                    # setup request to load next 500 edits
                    params['rvlimit'] = 500
                    params['rvcontinue'] = data['continue']['rvcontinue']

                # add each edit to the editData
                for edit in data['query']['pages'][str(self.id_)]['revisions']:
                    # convert timestamp string to a python datetime object
                    timestamp = datetime.strptime(edit['timestamp'], '%Y-%m-%dT%H:%M:%SZ')

                    # get revision ID, article size, and unix timestamp
                    revID = edit['revid']
                    size = edit['size']
                    unixtime = int(timestamp.timestamp())

                    # get user if included in data
                    user = 'unknown'
                    if 'user' in edit:
                        user = edit['user']

                    # break if timestamps are before 2015
                    if timestamp < datetime(2015, 1, 1):
                        done = True
                        break

                    editData.append([revID, size, unixtime, user])
        except:
            # record error when api retrieval fails
            print('Failed on num={}, id={}, title={}'.format(self.num, self.id_, self.title))
        return editData

class SubIndex:
    '''Create a sub collection of wikipedia articles with analysis data
    stored with it; by default, stored in <DATA_PATH>/SubIndexes/<name>'''

    def __init__(self, name, path=None, readOnly=False):
        '''create a new subindex or open an existing one with a given name'''

        # set basic data
        self.PATH = path
        if self.PATH is None:
            self.PATH = DATA_PATH / 'SubIndexes' / name
        self.readOnly = readOnly
        if not readOnly:
            self.dataPuller = DataPuller()
        self.name = name

        # check if the subindex exists; if not, build a new one
        if not self.PATH.exists():
            print("Subindex {} not found, creating in folder {}".format(name, self.PATH.as_posix()))
            self.PATH.mkdir()
        else:
            print("Subindex found at", self.PATH.as_posix())
        
        self.built = False # whether the index has been built with articles yet

        # core data structure attached to the subindex
        self.mainList = None # list of article numbers
        self.wikiIDList = None # list of wikiIDs
        self.mainDict = None # maps article numbers to mainList indexes
        self.titles = None # article titles

        # subviews of this subindex, held in their own SubIndex objects
        self.subIndexes = {}

        # extra data structures that can be calculated for the subindex
        self.linkCatalog = None # inter-article links
        self.pageRank = None # results of page-rank algorithm
        self.networkGraphData = None # layout info for network graphs
        self.viewCatalog = None # page view data
        self.editCatalog = None # edit data
        self.users = None # list of editors
        self.userEditCatalog = None # edit catalog lookup by user
        self.extraArticleInfo = None # extra computed for each article
        self.extraUserInfo = None # extra computed for each editor

        # load the main list if mainList.json exists
        if (self.PATH / 'mainList.json').exists():
            print("Loading data for subindex '{}'".format(self.name))
            self.loadMainList()
            self.built = True
    
    # Index Creation Functions

    def writeMainList(self):
        '''write the main list for the subindex'''

        mainList = []
        # combine the articlenumbers, wikiIDs, and titles into one list
        for i in range(len(self.mainList)):
            num = self.mainList[i]
            wikiID = self.wikiIDList[i]
            title = self.titles[i]

            mainList.append([num, wikiID, title])
    
        # write the information to a json file
        with open(self.PATH / 'mainList.json', 'w') as file:
            json.dump(mainList, file)

    def buildNew(self, nums, ignoreNums=None):
        '''build a new subindex with given article numbers, excluding anything in the set ignoreNums'''

        # remove duplicates and ignoreNums:
        nums = set(nums)
        if ignoreNums is not None:
            for ignore in ignoreNums:
                nums.discard(ignore)

        print("Building new main list for subindex '{}' with {} elements".format(self.name, len(nums)))

        self.mainList = [] # list of article nums
        self.wikiIDList = [] # list of wikiIDs
        self.mainDict = {} # map of article nums to mainList indexes
        self.titles = [] # the offsets of titles in the main index titleList

        # construct the main data structures
        index = 0
        for num in nums:
            info = self.dataPuller.getInfoFromNum(num)
            
            wikiID = info[0]
            titleOffset = info[1]
            title = self.dataPuller.getTitleAtOffset(titleOffset)

            self.mainList.append(num)
            self.wikiIDList.append(wikiID)
            self.titles.append(title)

            self.mainDict[num] = index   

            index += 1
        
        # write the indexes to disk
        print('Writing main list to disk...')
        self.writeMainList()
        print("Done building new list for subindex '{}'!".format(self.name))

        self.built = True

    def autoBuild(self, linkArticleTitle, subIndexMinSize=100):
        '''Auto build a subindex based on the links contained in a link article,
        and optionally build subindexes from the lists, glossaries, and indexes 
        in the listArticle'''

        nums = []
        # if multiple list articles passed scrape title from all of them
        if isinstance(linkArticleTitle, list):
            for articleTitle in linkArticleTitle:
                linkArticle = ArticleGetter(articleTitle)
                if linkArticle.num is None:
                    continue
                nums.extend(linkArticle.getLinkNums())
        # scrape articles where the list is split alphabetically
        # (eg see alphabet indices box in the article for "Mathematics")
        elif '{}' in linkArticleTitle:
            articleGroups = ['!$@', '0-9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','T','U','V','W','X','Y','Z']
            for articleGroup in articleGroups:
                linkArticle = ArticleGetter(linkArticleTitle.format(articleGroup))
                if linkArticle.num is None:
                    continue

                nums.extend(linkArticle.getLinkNums())
        # scrape links from a single list article
        else:
            linkArticle = ArticleGetter(title=linkArticleTitle)
            nums = linkArticle.getLinkNums()

        # do not include the list articles themselves
        ignoreNums = [linkArticle.num]
        
        # create the subindex and run extra functions
        self.buildNew(nums, ignoreNums=ignoreNums)
        self.buildLinkCatalog()
        self.loadLinkCatalog()
        self.autoBuildSubIndexes(criticalSize=subIndexMinSize)

    def buildFromCategory(self, categoryName, subIndexMinSize=100):
        '''Build a subindex with by collecting all of the articles from a category page;
        try finding a list article(s) and running autoBuild; category pages tend to
        include more off-topic articles'''

        # grab articles and subcategories from the category
        pages, subcategories = WikiCategoryScraper.scrapeWikiCategory('https://en.wikipedia.org/wiki/Category:' + categoryName)

        nums = [] # list for collecting all the article numbers
        for title in pages:
            # convert the titles to article numbers
            article = ArticleGetter(title)
            if article.num is not None:
                nums.append(article.num)
        
        subIndexes = []
        # grab the articles in each subcategory; no further recursion is done as the
        # articles become off-topic, and their numbers grow exponentially
        for i, category in enumerate(subcategories):
            #print('\r  Processing subcategory {}/{}'.format(i + 1, len(subcategories)), end='')
            pages, unused = WikiCategoryScraper.scrapeWikiCategory(subcategories[category])
            
            # convert all the titles to article numbers
            subNums = []
            for title in pages:
                article = ArticleGetter(title)
                if article.num is not None:
                    subNums.append(article.num)
            nums.extend(subNums)
            if len(subNums) >= subIndexMinSize:
                subIndexes.append([category.replace(' ', '_'), subNums])
        print()
    
        self.buildAll(nums)
        for subIndex in subIndexes:
            self.createSubIndex(subIndex[0], nums=subIndex[1])

    # additional data set retrieval/building functions

    def buildLinkCatalog(self):
        '''create a list of lists of all inter-article links in the subindex'''

        if not self.built:
            print('Error: cannot buid link catalog; subindex not built')
            raise Exception

        linkCatalog = [] # list for storing link catalog

        numsSet = set(self.mainList)
    
        print("Building link catalog for '{}'".format(self.name))
        nArticles = len(self.mainList)

        for index, num in enumerate(self.mainList):
            print('\r  Constructing links for article {}/{}      '.format(index + 1, nArticles), end='')

            linkNums = ArticleGetter(num=num).getLinkNums()
            if linkNums is None:
                linkCatalog.append([])
                continue
        
            # get only nums that are in the subindex w/ set intersection
            linkNums = numsSet & set(linkNums)

            # convert article numbers to mainlist indexes and sort
            linkIndexes = list(map(lambda x: self.mainDict[x], linkNums))
            linkIndexes.sort()

            # add link indexes to the linkCatalog list
            linkCatalog.append(linkIndexes)
        
        print()

        # write the link catalog to disk
        self.linkCatalog = linkCatalog
        with open(self.PATH / 'linkCatalog.json', 'w') as file:
            json.dump(self.linkCatalog, file)

        print("Done constructing link catalog for '{}'!".format(self.name))
    
    def buildPageRank(self, iters = 100):
        '''Run the PageRank algorithm on the articles in the index;
        this calculates a relative "importance" for articles based
        on inter-article links'''

        if self.linkCatalog is None:
            print("Error: Build and load the link catalog before running PageRank")
            return

        print('Calculating PageRank for Subindex "{}"'.format(self.name))

        n = len(self.linkCatalog) # the number of articles
        P = np.zeros(shape=(n,n)) # the PageRank matrix
        x = np.ones(shape=(n,1)) / n # the vector where the PageRank will be stored in

        # build the pageRank matrix
        print('  Building P matrix...')
        for i, links in enumerate(self.linkCatalog):
            nLinks = len(links)
            links = set(links)
            links.discard(i)

            # if an article doesn't link to anything, pretend that it links
            # to every other article (inlcuding itself)
            if nLinks == 0:
                for j in range(n):
                    P[j][i] = 1 / n
            else:
                for link in links:
                    P[link][i] = 1 / nLinks
        
        # run "iters" iterations to approximate the page rank
        print('  P matrix built, beginning to iterate')
        for i in range(iters):
            print('\r    performing iteration {}/{}'.format(i + 1, iters), end='')
            x = P @ x
        print()

        print('  Writing page rank to disk...')

        # write the page rank data to a json file
        self.pageRank = []
        for i in range(n):
            self.pageRank.append(x[i][0])
        with open(self.PATH / 'pageRank.json', 'w') as file:
            json.dump(self.pageRank, file)

        print('Page rank successfull built!')
    
    def buildNetworkGraphData(self):
        '''calculate the node locations for a network graph of the articles;
        This relies on the external program 'forceDirectedGraphMaker'; the
        cpp file for this program must be compiled, and the code below must
        be replaced with the correct way of calling the program'''

        # create data and offset files that the network graph program can read
        linkOffsetFile = open(self.PATH / 'linkOffsets.bin', 'wb')
        linkDataFile = open(self.PATH / 'linkData.bin', 'wb')

        # allows zero offset to be used for articles with no links
        linkDataFile.write(b'\xff\xff\xff\xff')
        numsSet = set(self.mainList)
        nArticles = len(self.mainList)

        # create the link offset files
        for links in self.linkCatalog:
            # point to the special zero offset if there are no outgoing links
            if len(links) == 0:
                linkOffsetFile.write(b'\x00\x00\x00\x00')
                continue
            else:
                # write the location that the links offsets can be found in the data file
                linkOffsetFile.write(linkDataFile.tell().to_bytes(4, 'little'))
            
            for link in links:
                linkDataFile.write(link.to_bytes(4, 'little'))
            linkDataFile.write(b'\xff\xff\xff\xff') # 0xFFFFFFFF marks end of links

        # close offset files
        linkOffsetFile.close()
        linkDataFile.close()

        # run the program that builds the force directed diagram
        raise NotImplementedError
        # functioning way of calling the program from a unix system
        #os.system('./forceDirectedGraphMaker ' + str(self.PATH.absolute()))

        # read the data that the forceDirectedGraphMaker produced
        pointLocations = []
        with open(self.PATH / 'networkGraphData.bin', 'rb') as pointLocFile:
            nPoints = len(self.mainList)
            for i in range(nPoints):
                # read the point data which are stored as integers between 0 and 10^16;
                # map each coordinate it back to the range -10 to 10
                x = float(int.from_bytes(pointLocFile.read(8), 'little')) / 1000000000000000 - 10.0
                y = float(int.from_bytes(pointLocFile.read(8), 'little')) / 1000000000000000 - 10.0
                pointLocations.append( (x,y) )
        
        # write the data to a json file for future use
        self.networkGraphData = pointLocations
        with open(self.PATH / 'networkGraphData.json', 'w') as file:
            json.dump(self.networkGraphData, file)

    def buildViewCatalog(self):
        '''Build a catalog of pageview data for each article using
        Wikipedia's pageview api and save this data'''
        
        nNums = len(self.mainList)
        viewCatalog = [[] for _ in range(nNums)]

        # the scraping function run by the thread pool
        def scrapeFunc(info):          
            numList = info['numList']
            iList = info['iList']
            nNums = len(numList)

            # the start and end date of page view data to scrape in the format
            # YYYYMMDDhh (note: data isn't available before the middle of 2015;
            # 2010 is set to ensure that the earlist possible data is pulled)
            startDate = '2010100100'
            endDate = '2020033100'

            while True: # run until the thread pool has exhausted all articles
                i = None
                with info['lock']:
                    # get another article from the job list
                    if len(iList) > 0:
                        i = iList.pop()
                        print('\r  Processing{}/{}'.format(i, nNums), end='')
                
                if i is None:
                    break

                # retry each request up to 100 times
                for j in range(100):
                    worked = False
                    try:
                        num = numList[i]
                        title = ArticleGetter(num=num).title.replace(' ', '_')

                        # make the api call
                        requestURL = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{}/monthly/{}/{}'.format(title, startDate, endDate)
                        text = requests.get(requestURL, headers={'User-Agent' : 'EG10112 Math Wikipedia Articles Analysis (bryaningwersen@outlook.com)'}).text

                        # pull the view data out of the json file returned
                        data = json.loads(text)
                        views = list(map(lambda x: x['views'], data['items']))
                        with info['lock']:
                            viewCatalog[i] = views
                            worked = True
                    except:
                        pass

                    if worked:
                        break
                else:
                    print("Failed", i)

        # build a thread pool to run requests in parallel
        scrapeList = [index for index in range(nNums)] # job list for thread pool
        tp = ThreadPool(scrapeFunc, {'iList' : scrapeList, 'numList' : self.mainList}, nThreads=40)
        tp.start()
        tp.waitAll()
        print('')
        
        # write the view data to a json file
        self.viewCatalog = viewCatalog
        with open(self.PATH / 'viewCatalog.json', 'w') as file:
            json.dump(self.viewCatalog, file)
    
    def buildEditCatalog(self):
        '''retrieve historical edit data for the last 5 years for each article'''

        userDict = {} # dict mapping usernames to user numbers
        userList = [] # list of user names in sequence by user number
        editCatalog = [] # list of lists of edit data for each article

        for i, num in enumerate(self.mainList):
            print('\r processing num={} ({}/{})         '.format(num, i+1, len(self.mainList)), end='')

            # get the edit history for the article
            editData = ArticleGetter(num=num).getEditHistory()

            for edit in editData:

                # convert the user's name into a user id
                user = edit[3]
                userNum = None
                if user in userDict:
                    userNum = userDict[user]
                else:
                    userNum = len(userList)
                    userDict[user] = userNum
                    userList.append(user)

                # add the revision ID, article size, timestamp, and userID
                editCatalog.append([edit[0], edit[1], edit[2], userNum])

        print()          

        self.editCatalog = editCatalog
        self.users = userList

        # write the edit data to json files
        with open(self.PATH / 'editCatalog.json', 'w') as file:
            json.dump(self.editCatalog, file)
        with open(self.PATH / 'users.json', 'w') as file:
            json.dump(self.users, file)
    
    def buildUserEditCatalog(self):
        '''build a catalog of each user's edits and write it to disk'''

        self.loadEditCatalog()

        # store each user's edits in a list of lists
        userEditCatalog = [[] for user in range(len(self.users))]

        for articleNum, articleEdits in enumerate(self.editCatalog):
            for editNum, edit in enumerate(articleEdits):
                userNum = edit[3]
                
                # store the article and edit numbers so that edit info
                # can be looked up from self.editCatalog
                userEditCatalog[userNum].append([articleNum, editNum])
        
        self.userEditCatalog = userEditCatalog
        with open(self.PATH / 'userEditCatalog.json', 'w') as file:
            json.dump(self.userEditCatalog, file)

    def buildExtraArticleInfo(self):
        '''This is a catalog of extra information where there is an entry
        for each article; it is intended for metrics which are expensive to
        compute and would be more reasonable to load from disk every time'''

        self.loadLinkCatalog()
        self.loadPageRank()
        self.loadViewCatalog()
        self.loadEditCatalog()

        extraArticleInfo = {
            'averageMonthlyPageViews' : self.getAverageMonthlyPageViews(),
            'uniqueEditors' : self.getUniqueEditors(),
            'flaggedEdits' : self.getFlaggedEdits(),
            'studentReferenceIndex' : self.getStudentReferenceIndex(),
            'backlinks' : self.getBacklinks(),
            'articleSizes' : self.getArticleSizes()
        }

        self.extraArticleInfo = extraArticleInfo
        with open(self.PATH / 'extraArticleInfo.json', 'w') as file:
            json.dump(self.extraArticleInfo, file)

    def buildExtraUserInfo(self):
        '''This is a catalog of extra information where there is an entry
        for each user; it is intended for metrics which are expensive to
        compute and would be more reasonable to load from disk every time'''

        extraUserInfo = {
            'flaggedEdits' : self.getUserFlaggedEdits(),
            'editSize' : self.getUserEditSize(),
            'editSizeNoFlagged' : self.getUserEditSize(excludeFlagged=True),
        }

        self.extraUserInfo = extraUserInfo
        with open(self.PATH / 'extraUserInfo.json', 'w') as file:
            json.dump(self.extraUserInfo, file)

    def buildAll(self, nums):
        '''build a subindex and calculate other useful information'''

        self.buildNew(nums)
        self.buildLinkCatalog()
        self.loadLinkCatalog()
        self.buildPageRank()

    # data cleaning/analysis/printing functions

    def getBacklinks(self):
        '''calculate backlinks for all articles (the link catalog
        contains outgoing links from articles; this creates a list
        of incoming links for each article)'''

        if self.linkCatalog is None:
            print('Error: link catalog must be loaded to get backlinks')
            return
        
        # add each link in the link catalog to its spot in the backlink list
        backlinks = [[] for i in range(len(self.linkCatalog))]
        for i, links in enumerate(self.linkCatalog):
            for link in links:
                backlinks[link].append(i)
        return backlinks
    
    def getHighestRankedPages(self, n=50, display=False):
        '''Return a list of the indexes of the top n highest ranked
        articles by page rank and optionall display the info'''

        if self.pageRank is None or self.linkCatalog is None:
            print('Error: page rank & link catalog must be loaded to get highest ranked pages')
            return
        
        rankings = [] # store tuples of form (articleIndex, pageRank)
        for i, rank in enumerate(self.pageRank):
            rankings.append((i, rank))
        
        # sort and optionally display highest ranked pages
        rankings.sort(key=lambda x: x[1], reverse=True)
        if display:
            for i in range(n):
                index = rankings[i][0]
                rating = rankings[i][1]

                num = self.mainList[index]
                article = ArticleGetter(num=num)

                out = "{}) (PageRank={}, index={}, wikiID={}):".format(i+1, rating, index, article.id_)
                out += (80 - len(out)) * " "
                out += article.title
                print(out)
        
        # retrieve the article indexes for the top n entries
        return list(map(lambda x:x[0], rankings[:n]))
    
    def printBacklinksForIndex(self, index):
        '''display the titles of the backlinks for a particular
        article given its index'''

        backlinks = self.getBacklinks()

        print("Backlinks for article w/ index", index)
        backlinks = backlinks[index]
        for link in backlinks:
            article = ArticleGetter(num=self.mainList[link])
            print("   ", article.title)
    
    def getArticleSizes(self):
        '''return a list of the size of articles (in characters) in the index'''

        articleSizes = []
        for num in self.mainList:
            size = len(ArticleGetter(num=num).getText())
            articleSizes.append(size)
        
        return articleSizes
    
    def getTotalEdits(self):
        '''return the number of edits in the last 5 years for each article
        in the index'''

        totalEdits = []
        for articleEdits in self.editCatalog:
            totalEdits.append(len(articleEdits))
        return totalEdits
    
    def getAverageMonthlyPageViews(self):
        '''return the average number of page views per month for each article
        in the index'''

        averagePageViews = []
        for articleViews in self.viewCatalog:
            nMonths = len(articleViews)
            totalViews = sum(articleViews)
            if totalViews != 0:
                averagePageViews.append(totalViews / nMonths)
            else:
                averagePageViews.append(0.0)
        
        return averagePageViews
    
    def getUniqueEditors(self):
        '''return the number of unique editors for each article in the index'''

        uniqueEditors = []
        for articleEdits in self.editCatalog:
            editors = set()
            for edit in articleEdits:
                userNum = edit[3]
                editors.add(userNum)
            uniqueEditors.append(list(editors))

        return uniqueEditors
    
    def getFlaggedEdits_old(self):
        '''get the indexes of flagged edits for each article in the index;
        an edit is flagged if it appears to be malicious, aka a large
        insertion or deletion was made and then reverted'''

        flaggedEdits = [] # combined list of flagged edits

        for articleEdits in self.editCatalog:
            if len(articleEdits) == 0:
                flaggedEdits.append([])
                continue
            
            lastEditSize = articleEdits[0][1]
            possibleFlaggedEditSize = None # article size before a possibly flagged edit
            possibleFlaggedEdits = [] # list of possible but unconfirmed flagged edits

            articleFlaggedEdits = [] # list of confirmed flagged edits
            
            for index, edit in enumerate(articleEdits):
                if index == 0:
                    continue

                editSize = edit[1]

                # a change of less than 1000 bytes will not be flagged
                if abs(editSize - lastEditSize) < 1000:
                    # clear any possible flagged edits
                    lastEditSize = editSize
                    possibleFlaggedEdits = []
                    possibleFlaggedEditSize = None
                
                # check if this edit reverted possible flagged edits
                elif (
                    (possibleFlaggedEditSize is not None) and
                    (abs(editSize - possibleFlaggedEditSize) < 100)
                ):
                    articleFlaggedEdits.extend(possibleFlaggedEdits)
                    possibleFlaggedEdits = []
                    possibleFlaggedEditSize = None
                
                # mark the edit as a possible flagged edit
                else:
                    if possibleFlaggedEditSize is None:
                        possibleFlaggedEditSize = lastEditSize
                    possibleFlaggedEdits.append(index)
                
                lastEditSize = editSize

            flaggedEdits.append(articleFlaggedEdits)
        
        return flaggedEdits

    def getFlaggedEdits(self):
        '''get the indexes of flagged edits for each article in the index;
        an edit is flagged if it appears to be malicious, aka a large
        insertion or deletion was made and then reverted'''

        flaggedEdits = [] # combined list of flagged edits

        for articleEditsBackwards in self.editCatalog:
            articleEdits = list(reversed(articleEditsBackwards))
            if len(articleEdits) == 0:
                flaggedEdits.append([])
                continue
            
            lastSize = articleEdits[0][1]
            articleFlaggedEdits = [] # list of confirmed flagged edits
            
            for index, edit in enumerate(articleEdits):
                if index == 0:
                    continue

                newSize = edit[1]

                # check if edits larger than 1000 characters were reverted
                if abs(newSize - lastSize) >= 1000:
                    editorID = edit[3]
                    editDateTime = edit[2]

                    # check if the edit was reverted within a day
                    for futureIndex in range(index + 1, len(articleEdits)):
                        futureEdit = articleEdits[futureIndex]
                        futureSize = futureEdit[1]
                        futureDateTime = futureEdit[2]

                        # check if edit was more than a day old
                        if futureDateTime - editDateTime > 86400:
                            break
                        
                        # flag the edit if it completely reverted
                        if futureSize - lastSize == 0:
                            # mark all intermediate edits by the same editor
                            newFlaggedEdits = [(len(articleEdits) - i - 1) for i in range(index, futureIndex) if articleEdits[i][3] == editorID]
                            articleFlaggedEdits.extend(newFlaggedEdits)
                            break
                
                lastSize = newSize

            # remove duplicates
            articleFlaggedEdits = list(set(articleFlaggedEdits))
            flaggedEdits.append(articleFlaggedEdits)
        
        return flaggedEdits

    def getStudentReferenceIndex(self):
        '''This returns a metric calculated for each article that guesses the 
        relative extent that the Wikipedia article is used as reference
        material by students; this is done by comparing the number of page
        views an article gets during months when schools are normally in
        session relative to when they are not'''

        def filteredMean(numbers):
            '''return the mean of numbers that are within 1.2 standard deviations
            of the mean of all the numbers'''

            if len(numbers) == 1:
                return numbers[0]

            mean = sum(numbers) / len(numbers)
            stdDev = math.sqrt(sum([(x-mean)*(x-mean) for x in numbers]) / (len(numbers) - 1))
            
            filteredNums = [x for x in numbers if abs(x - mean) <= stdDev * 1]
            return sum(filteredNums) / len(filteredNums)

        studentRefIndexes = []
        for views in self.viewCatalog:
            # go backward through time and go by calendar year
            # remove Jan, Feb, and March from current year
            views = list(reversed(views))[3:]
            ratios = []
            while len(views) >= 12:
                inSchoolViews = filteredMean([views[1], views[2], views[8], views[9]]) # Nov, Oct, Apr, Mar
                outOfSchoolViews = (views[5] + views[6]) / 2 # Jul, Jun

                if outOfSchoolViews != 0:
                    ratios.append(inSchoolViews / outOfSchoolViews)

                views = views[12:] # go back one more year

            if len(ratios) == 0:
                studentRefIndexes.append(1.0)
            else:
                studentRefIndexes.append(filteredMean(ratios))
        
        return studentRefIndexes

    def getUserFlaggedEdits(self):
        '''build a list of flagged edits for each user'''
        
        self.loadExtraArticleInfo()
        self.loadUserEditCatalog()

        # list of all flagged edits by article
        flaggedEdits = self.extraArticleInfo['flaggedEdits']
        flaggedEdits = [set(edits) for edits in flaggedEdits]

        # check each users' edits against the flagged edits list
        userFlaggedEdits = []
        for userEdits in self.userEditCatalog:
            thisUsersFlagged = []

            for num, edit in enumerate(userEdits):
                articleNum = edit[0]
                editNum = edit[1]

                # check if this edit was flagged
                if editNum in flaggedEdits[articleNum]:
                    thisUsersFlagged.append(num)
            
            userFlaggedEdits.append(thisUsersFlagged)

        return userFlaggedEdits

    def getUserEditSize(self, excludeFlagged=False):
        '''build a list of each user's total contributions
        (total edit additions and total edit deletions);
        optionally exclude flagged edits from their totals'''

        def getEditSize(articleNum, editNum):
            # when the edit is the first one recorded in the data
            # set, its size is unknown
            if editNum + 1 >= len(self.editCatalog[articleNum]):
                return 0
            
            previousSize = self.editCatalog[articleNum][editNum + 1][1]
            newSize = self.editCatalog[articleNum][editNum][1]

            return newSize - previousSize

        self.loadEditCatalog()
        self.loadUserEditCatalog()

        userFlaggedEdits = None
        if excludeFlagged:
            userFlaggedEdits = self.getUserFlaggedEdits()

        userEditSizes = []
        for userNum, userEdits in enumerate(self.userEditCatalog):
            addedSize = 0
            removedSize = 0
            for num, edit in enumerate(userEdits):
                if excludeFlagged and (num in userFlaggedEdits[userNum]):
                    continue
                
                size = getEditSize(edit[0], edit[1])
                if size > 0:
                    addedSize += size
                else:
                    removedSize += -size
            
            userEditSizes.append([addedSize, removedSize])
        
        return userEditSizes

    # SubIndex creation Functions

    def createSubIndex(self, name, nums=None, indexes=None, ignoreNums={}):
        '''build a subIndex of the subIndex; either provide the article numbers
        or provide a list of indexes in the current (self) subIndex; the data is
        stored in a sub-directory of the current subIndexes main directory'''

        # build the set of article numbers and purge articles
        # that are not in the current (self) subIndex
        if nums is not None:
            nums = set(nums) & set(self.mainList)
        elif indexes is not None:
            nums = set(map(lambda x:self.mainList[x], indexes))
        else:
            print('Error: must provide nums or indexes when building sub-subindex')
            return
        
        # build and save a new, bare subindex
        subIndex = SubIndex(name, path=(self.PATH / name))
        subIndex.buildNew(list(nums), ignoreNums=ignoreNums)
        
        # build the link catalog by leveraging the parent (self) index's
        # link catalog; this is much more efficient than running
        # subIndex.buildLinkCataolog() because it doesn't have to load
        # and decompress each article's text from disk
        linkCatalog = []
        for num in subIndex.mainList:
            parentIndex = self.mainDict[num]
            parentLinks = self.linkCatalog[parentIndex]

            # start with the inter-article links of the parent subIndex and
            # purge links that are not in the new subIndex
            linkNums = set(map(lambda x: self.mainList[x], parentLinks)) & nums

            # convert the link indexes of the parent subIndex into the link
            # indexes of the new indexes
            linkIndexes = list(map(lambda x: subIndex.mainDict[x], linkNums))
            linkIndexes.sort()
            linkCatalog.append(linkIndexes)

            # write the data in the same format described in 'buildLinkCatalog()'
            linkCatalog.append(linkNums)
        
        # write the link catalog
        subIndex.linkCatalog = linkCatalog
        with open(subIndex.PATH / 'linkCatalog.json', 'w') as file:
            json.dump(subIndex.linkCatalog, file)

        self.subIndexes[name] = subIndex
    
    def autoBuildSubIndexes(self, criticalSize=100):
        '''automatically build sub-subindexes; find Wikipedia list, glossary
        and index pages in the subindex and build a sub subindex based on it
        if it contains at least "criticalSize" articles'''

        if self.linkCatalog is None:
            print("Error: link catalog must be loaded to build SubIndexes")
            return

        total = []
        for i,num in enumerate(self.mainList):
            # search for wikipedia list, glossary, and article index articles
            # within the current subindex
            article = ArticleGetter(num=num)
            title = article.title
            if title.startswith('List of') or title.startswith('Glossary of') or title.startswith('Index of'):

                links = self.linkCatalog[i]
                if len(links) > criticalSize:
                    total.extend(links)

                    # convert the list/glossary/index article title into
                    # a consistently formatted sub-subindex name
                    start = title.find('of ') + 3
                    title = title[start:] # remove 'List of'/'Glossary of'/'Index of'
                    title = title.replace(',', ' ') # replace comma's and dashses w/ spaces
                    title = title.replace('-', ' ')
                    title = title.replace('articles', '') # remove generic words
                    title = title.replace('topics', '')

                    # replace spaces with '_' and uppercase words
                    title = title.split()
                    for i in range(len(title)):
                        word = title[i]
                        word = word[0].upper() + word[1:]
                        title[i] = word
                    title = '_'.join(title)

                    self.createSubIndex(title, indexes=links)

    # Loading Functions

    def loadMainList(self):
        '''load the main indexes from disk into python data structures'''

        with open(self.PATH / 'mainList.json') as file:
            mainList = json.load(file)
        
            self.mainList = []
            self.wikiIDList = []
            self.titles = []
            self.mainDict = {} # main numbers -> # subindex ids

            for index, row in enumerate(mainList):
                # load the data fields from the binary file
                num = row[0]
                wikiID = row[1]
                title = row[2]

                # add the values to the python data structures
                self.mainList.append(num)
                self.wikiIDList.append(wikiID)
                self.titles.append(title)
                self.mainDict[num] = index

    def loadLinkCatalog(self):
        '''Load the subindex's link catalog from a file on disk'''

        # check if the catalog is already loaded
        if self.linkCatalog is not None:
            print("Link catalog already loaded")
            return
        
        # check that the file exists on the disk
        if not (self.PATH / 'linkCatalog.json').exists():
            print("No link catalog found: run self.buildLinkCatalog() to create")
            return
    
        print('Loading link catalog for subindex "{}"'.format(self.name))

        # load json file
        with open(self.PATH / 'linkCatalog.json') as file:
            self.linkCatalog = json.load(file)
        
        print('Link catalog loaded!')

    def loadPageRank(self):
        '''load a pageRank that has already been built from disk'''

        # check if the pageRank is already loaded into a python list
        if self.pageRank is not None:
            print('Page rank already loaded!')
            return
        
        # check that the file exists
        if not (self.PATH / 'pageRank.json').exists():
            print('Page rank not created for this subindex; run buildPageRank to create')
            return
        
        # load json file
        with open(self.PATH / 'pageRank.json') as file:
            self.pageRank = json.load(file)
        
        print('Page rank loaded!')

    def loadViewCatalog(self):
        '''load view data that was saved to disk'''

        if self.viewCatalog is not None:
            return self.viewCatalog

        # load json file
        with open(self.PATH / 'viewCatalog.json') as file:
            self.viewCatalog = json.load(file)

    def loadEditCatalog(self):
        '''load the edit catalog data saved by createEditCatalog()'''

        if self.editCatalog is not None and self.users is not None:
            return
        
         # load json file
        with open(self.PATH / 'editCatalog.json') as file:
            self.editCatalog = json.load(file)
        with open(self.PATH / 'users.json') as file:
            self.users = json.load(file)

    def loadUserEditCatalog(self):
        '''load the edit catalog data saved by createEditCatalog()'''

        if self.userEditCatalog is not None:
            return
        
         # load json file
        with open(self.PATH / 'userEditCatalog.json') as file:
            self.userEditCatalog = json.load(file)

    def loadExtraArticleInfo(self):
        '''load the extra article info data sets'''

        if self.extraArticleInfo is not None:
            return
        
        # load json file
        with open(self.PATH / 'extraArticleInfo.json') as file:
            self.extraArticleInfo = json.load(file)

    def loadExtraUserInfo(self):
        '''load the extra article info data sets'''

        if self.extraUserInfo is not None:
            return
        
        # load json file
        with open(self.PATH / 'extraUserInfo.json') as file:
            self.extraUserInfo = json.load(file)

    def loadNetworkGraphData(self):
        '''load the location of each article's representation in a
        network graph as calculated by forceDirectedGraphMaker; the
        location is a point (x,y) within 10 units of the origin'''

        if self.networkGraphData is not None:
            return self.networkGraphData

        if not (self.PATH / 'networkGraphData.json').exists():
            print('Network graph for index {} doesn\'t exist'.format(self.name))
            print('To build, run ./forceDirectedGraphMaker {}'.format(self.name))
            return

        with open(self.PATH / 'networkGraphData.json') as file:
            self.networkGraphData = json.load(file)

    def loadSubIndex(self, name):
        '''load a sub-subindex by name'''

        path = self.PATH / name
        if path.is_dir():
            subIndex = SubIndex(name, path=path, readOnly=self.readOnly)
            self.subIndexes[name] = subIndex
            return subIndex
        else:
            print("Error: cannot open subindex '{}': directory not found".format(name))
            return None

    def loadAllSubIndexes(self):
        '''load all of the subindex's sub-subindexes'''

        dirContents = os.listdir(self.PATH)
        for entry in dirContents:
            if (self.PATH / entry).is_dir():
                self.loadSubIndex(entry)
    
    # data accessor system; this system provides a unifed interface for accessing
    # data sets associated with the index; it is recomended that external modules
    # that only read data do so with these functions
    def setupDataAccessorSystem(self):
        '''load and setup the data structures that back the data accessor system'''

        # master object with python data structures of raw data
        self.dataCatalog = None

        # load all of the data stored on disk
        self.loadLinkCatalog()
        self.loadPageRank()
        self.loadViewCatalog()
        self.loadEditCatalog()
        self.loadUserEditCatalog()
        self.loadNetworkGraphData()
        self.loadExtraArticleInfo()
        self.loadExtraUserInfo()

        self.dataCatalog = {
            'articles' : {
                'Article Numbers' : self.mainList,
                'Article Wiki IDs' : self.wikiIDList,
                'Titles' : self.titles,
                'Outgoing Links' : self.linkCatalog,
                'Number of Outgoing Links' : [len(links) for links in self.linkCatalog],
                'Page Rank' : self.pageRank,
                'Monthly Page Views' : self.viewCatalog,
                'Edits' : self.editCatalog,
                'Number of Edits' : [len(edits) for edits in self.editCatalog],
                'Network Graph Locations' : self.networkGraphData,
                'Average Monthly Page Views' : self.extraArticleInfo['averageMonthlyPageViews'],
                'Editors' : self.extraArticleInfo['uniqueEditors'],
                'Number of Editors' :  [len(editors) for editors in self.extraArticleInfo['uniqueEditors']],
                'Flagged Edits' :  self.extraArticleInfo['flaggedEdits'],
                'Number of Flagged Edits' : [len(edits) for edits in self.extraArticleInfo['flaggedEdits']],
                'Student Reference Index' : self.extraArticleInfo['studentReferenceIndex'],
                'Incoming Links' : self.extraArticleInfo['backlinks'],
                'Number of Incoming Links' : [len(links) for links in self.extraArticleInfo['backlinks']],
                'Article Size' : self.extraArticleInfo['articleSizes'],
            },
            'editors' : {
                'Names' : self.users,
                'Edits' : self.userEditCatalog,
                'Number of Edits' : [len(edits) for edits in self.userEditCatalog],
                'Flagged Edits' : self.extraUserInfo['flaggedEdits'],
                'Number of Flagged Edits' : [len(edits) for edits in self.extraUserInfo['flaggedEdits']],
                'Additions' : [changes[0] for changes in self.extraUserInfo['editSize']],
                'Deletions' : [changes[1] for changes in self.extraUserInfo['editSize']],
                'Changes' : [changes[0] + changes[1] for changes in self.extraUserInfo['editSize']],
                'Non-Flagged Additions' : [changes[0] for changes in self.extraUserInfo['editSizeNoFlagged']],
                'Non-Flagged Deletions' : [changes[1] for changes in self.extraUserInfo['editSizeNoFlagged']],
                'Non-Flagged Changes' : [changes[0] + changes[1] for changes in self.extraUserInfo['editSizeNoFlagged']],
            }
        }

        print("Data Accessor System Setup Complete!")

    def dataAccessor(self, type_, dataSet):
        '''get a data set of information for each article (type_='articles') or
        information for each editor (type='editors')'''

        return self.dataCatalog[type_][dataSet]

    def categoryDataAccessor(self, dataSet, category=None):
        '''get an 'articles' data set for all the articles in a specific category;
        if no category is specified, then data for all articles is returned'''

        data = self.dataAccessor('articles', dataSet)
        if category is None:
            return data
        
        # filter the data for only articles in the specified category
        nums = self.subIndexes[category].mainList
        newData = []
        for num in nums:
            index = self.mainDict[num]
            newData.append(data[index])
        
        return newData
    
    def articleDataAccessor(self, dataSet, articleIndex):
        '''get the information for a single article from a data set'''

        return self.categoryDataAccessor(dataSet, None)[articleIndex]
