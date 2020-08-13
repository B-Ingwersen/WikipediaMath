# wikiDataIndexer.py
# This program uses the data donwloaded by wikiDownloader and creates
# a number of indexing files for the later parts of the data analysis
# to use
#
# Author: Bryan Ingwersen
# Date: 28 March, 2020

import os
import json
import bz2
from pathlib import Path

# relative path to where the data was dumped
DATA_PATH = Path() / 'data' / 'wikiDump'
# the date string of the dump downloaded YYYYMMDD
DUMP_DATE = '20200301'

fileIndex = None
def buildFileIndex():
    '''create a json file that catalogues all of the dump files and
    save a version of this data in the global variable fileIndex'''
    global fileIndex

    # check if the fileIndex is already built; if yes load the file
    # instead of rebuilding it
    if (DATA_PATH / 'fileIndex.json').exists():
        with open(DATA_PATH / 'fileIndex.json') as file:
            fileIndex = json.load(fp=file)
        print("File index already present (delete 'fileIndex.json' to force file index rebuild)")
        return

    def indexFilenameGetPageRange(name):
        '''helper function that returns the page id range for a data file'''

        # check for a malformed file name
        i = name.find('.txt-p')
        if i == -1:
            print("Malformed index filename '{}'".format(name))
            raise ValueError

        # find the starting page id string
        startIndex1 = i + len('.txt-p')
        endIndex1 = name.find('p', startIndex1)
        if endIndex1 == -1:
            print("Malformed index filename '{}'".format(name))
            raise ValueError
    
        # fine the ending page id string
        startIndex2 = endIndex1 + 1
        endIndex2 = name.find('.bz2')

        # convert the page id strings into integers
        pageRangeStart = None
        pageRangeEnd = None
        try:
            pageRangeStart = int(name[startIndex1:endIndex1])
            pageRangeEnd = int(name[startIndex2:endIndex2])
        except:
            print("Malformed index filename '{}'".format(name))
            raise ValueError
    
        return pageRangeStart, pageRangeEnd
    
    # get all the dump index files
    fileNames = os.listdir(DATA_PATH)
    indexFileNames = []
    for fileName in fileNames:
        if fileName.startswith('enwiki-' + DUMP_DATE + '-pages-articles-multistream-index') and fileName.endswith('.bz2'):
            indexFileNames.append(fileName)

    print('BuildFileIndex: found {} index files'.format(len(indexFileNames)))

    # find all the corresponding data files for the index files
    catalog = []
    for file in indexFileNames:
        xmlFilename = file.replace('-index', '')
        xmlFilename = xmlFilename.replace('.txt', '.xml')
        if xmlFilename not in fileNames:
            print("Matching XML file for index file '{}' not found".format(file))
            raise FileNotFoundError

        start, end = indexFilenameGetPageRange(file)
        catalog.append({
            'start' : start,
            'end' : end,
            'indexFilename' : file,
            'xmlFilename' : xmlFilename
        })
    catalog.sort(key=lambda entry: entry['start'])

    # save the catalog to a python object and write it to fileIndex.json
    fileIndex = {
        'catalog' : catalog
    }
    with open(DATA_PATH / 'fileIndex.json', 'w') as file:
        json.dump(fileIndex, fp=file, indent=4)

def catalog_loadIndex(number):
    '''load and decompress an index file with a given number'''
    fileName = fileIndex['catalog'][number]['indexFilename']

    fileContents = None
    with bz2.BZ2File(DATA_PATH / fileName, 'r') as indexFile:
        fileContents = indexFile.readlines()
    
    return fileContents

def catalog_parseIndex(number):
    '''get the block information for one the index files;
    the xml files are compressed in seperate streams of 100
    article blocks; the index file contains the location of
    blocks and the titles of the articles contained in each
    block'''

    blocks = [] # list of all the blocks found in the index
    currentBlock = None

    indexData = catalog_loadIndex(number)
    for lineBytes in indexData:
        line = lineBytes.decode('utf-8')

        # parse the line which is of the format block_start:article_id:title
        sepIndex1 = line.find(':')
        sepIndex2 = line.find(':', sepIndex1 + 1)
        xmlBlockOffset = int(line[:sepIndex1])
        articleID = int(line[sepIndex1 + 1: sepIndex2])
        articleTitle = line[sepIndex2 + 1: -1]

        # add a new block entry if the block_offset is different from the last one
        if currentBlock is None or currentBlock['start'] != xmlBlockOffset:
            if currentBlock is not None:
                # record the starting address of the new block as the ending
                # address of the last block
                currentBlock['end'] = xmlBlockOffset
                blocks.append(currentBlock) # add the block to block list
            
            currentBlock = { # create a new block
                'start' : xmlBlockOffset,
                'end' : -1,
                'ids' : [],
                'titles' : []
            }
        
        # record the article's title and id in the current block
        currentBlock['ids'].append(articleID)
        currentBlock['titles'].append(articleTitle)
    
    blocks.append(currentBlock) # add the last block being constructed
    return blocks

def stringHashFunc(s):
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
        # (prevent C-Python from switching to large integer internal format)
        p &= 0x0003FFFFFFFFFFFF
    
    # perform a modulo by a large prime number to restrict size of hash
    return hash_ % 982451653

# static size of the hash maps to be built
HASH_MAP_SIZE = 6000000

def buildTitleHashTable(MAIN_INDEX_PATH):
    '''
    construct a hash table where all the articles titles are keys
    and the values are information about the article's location in
    the dump files; the hash map is then written to disk for future use
    '''

    print("  Creating title hash buckets...")
    titleHashes = [[] for i in range(HASH_MAP_SIZE)] # each subarray is a hash bucket

    articleNum = 0
    blockNum = 0
    nCatalogEntries = len(fileIndex['catalog'])

    # hash the articles in each index file
    for catalogIndex in range(nCatalogEntries):
        print("  Processing catalog entry {}/{}        ".format(catalogIndex + 1, nCatalogEntries))
        print("    Loading & parsing catalog entry...")

        blocks = catalog_parseIndex(catalogIndex) # load the block info for the index

        print("    Adding catalog entries to hash maps...")

        nBlocks = len(blocks)
        blockNum = 0
        
        for block in blocks: # hash the articles in each block of the index
            blockNum += 1
            print("\r      Working on block {}/{}".format(blockNum, nBlocks), end='')
            titles = block['titles']
            for i in range(len(titles)): # hash each title in the block
                title = titles[i]

                titleBucket = stringHashFunc(title) % HASH_MAP_SIZE
                titleHashes[titleBucket].append([title, articleNum])

                articleNum += 1
        
        print("\n      Total articles processed so far:", articleNum)

    # create the output files; titleHash.bin hash stores offsets
    # to where each bucket's data is stored in titleList.bin
    titleHashFile = open(MAIN_INDEX_PATH / 'titleHash.bin', 'wb')
    titleListFile = open(MAIN_INDEX_PATH / 'titleList.bin', 'wb')

    # record the offset of the titles to be returned for other functions
    print('  Building title offsets list...')
    titleOffsets = [-1 for i in range(articleNum)]

    print('  Writing title hash table...')
    for bucket in titleHashes:
        if len(bucket) > 0:
            # add the offset of the bucket to titleHash.bin
            titleHashFile.write(titleListFile.tell().to_bytes(4, 'little'))

            # write each bucket in the format:
            #   For each article:
            #       -utf-8 string title, uint8 0,
            #       -uint32 article number
            #   uint8 0
            for entry in bucket:
                title = entry[0].encode('utf-8')
                num = entry[1]

                # save the offset of the title in the file
                offset = titleListFile.tell()
                titleOffsets[num] = offset

                titleListFile.write(title)
                titleListFile.write(b'\x00')
                titleListFile.write(num.to_bytes(4, 'little'))
            titleListFile.write(b'\x00')
        else:
            # write 0xFFFFFFFF to signify that the bucket is empty
            titleHashFile.write(b'\xff\xff\xff\xff')

    # close file descriptors
    titleHashFile.close()
    titleListFile.close()

    # explicitly delete the hash map so hopefully the CPython garbage collector
    # will free up memory
    print('  Cleaning up title hash data...')
    del titleHashes

    return titleOffsets

def buildIdHashTable(MAIN_INDEX_PATH):
    '''
    construct a hash table where all the keys are wikipedia article
    ids and the values are information about the article's location in
    the dump files; the hash map is then written to disk for future use
    '''

    print("  Creating id hash bucket...")
    idHashes = [[] for i in range(HASH_MAP_SIZE)] # each subarray is a hash bucket

    articleNum = 0
    blockNum = 0
    nCatalogEntries = len(fileIndex['catalog'])

    # hash the articles in each index
    for catalogIndex in range(nCatalogEntries):
        print("  Processing catalog entry {}/{}        ".format(catalogIndex + 1, nCatalogEntries))
        print("    Loading & parsing catalog entry...")

        blocks = catalog_parseIndex(catalogIndex) # load the block info for the index

        print("    Adding catalog entries to hash maps...")

        nBlocks = len(blocks)
        blockNum = 0
        
        for block in blocks: # hash the articles in each block of the index
            blockNum += 1
            print("\r      Working on block {}/{}".format(blockNum, nBlocks), end='')
            ids = block['ids']
            for i in range(len(ids)): # hash each article id in the block
                id_ = ids[i]

                idBucket = id_ % HASH_MAP_SIZE
                idHashes[idBucket].append([id_, articleNum])

                articleNum += 1

        print("\n      Total articles processed so far:", articleNum)

    # create the output files; idHash.bin hash stores offsets
    # to where each bucket's data is stored in idHash.bin
    idHashFile = open(MAIN_INDEX_PATH / 'idHash.bin', 'wb')
    idListFile = open(MAIN_INDEX_PATH / 'idList.bin', 'wb')

    print('  Writing id hash table...')
    for bucket in idHashes:
        if len(bucket) > 0:
            # add the offset of the bucket to idHash.bin
            idHashFile.write(idListFile.tell().to_bytes(4, 'little'))

            # write each id as a 32 bit integer followed with the
            # end of the bucket marked by 0xFFFFFFFF
            for entry in bucket:
                id_ = entry[0].to_bytes(4, 'little')
                num = entry[1]

                idListFile.write(id_)
                idListFile.write(num.to_bytes(4, 'little'))
            idListFile.write(b'\xff\xff\xff\xff')
        else:
            # write 0xFFFFFFFF to signify that the bucket is empty
            idHashFile.write(b'\xff\xff\xff\xff')
    
    # close file descriptors
    idHashFile.close()
    idListFile.close()

    # explicitly delete the hash map so hopefully the CPython
    # garbage collector will free up memory
    print('  Cleaning up id hash data...')
    del idHashes

def buildMainList(MAIN_INDEX_PATH, titleOffsets):
    '''
    create a main list file that acts as a random access list for
    finding article ids, title, and block location
    '''

    # open main list file descriptor
    print('  Writing main list...')
    mainListFile = open(MAIN_INDEX_PATH / 'mainList.bin', 'wb')

    articleNum = 0
    overallBlockNum = 0
    nCatalogEntries = len(fileIndex['catalog'])

    # iterate through each index file
    for catalogIndex in range(nCatalogEntries):
        print("  Processing catalog entry {}/{}        ".format(catalogIndex + 1, nCatalogEntries))
        print("    Loading & parsing catalog entry...")

        blocks = catalog_parseIndex(catalogIndex)

        print("    Adding catalog entries to hash maps...") # load the block info for the index

        nBlocks = len(blocks)
        blockNum = 0
        
        for block in blocks: # iterate through each block of the index file
            blockNum += 1
            print("\r      Working on block {}/{}".format(blockNum + 1, nBlocks), end='')

            ids = block['ids']
            for i in range(len(ids)): # iterate through each id in the block
                id_ = ids[i]

                titleOffset = titleOffsets[articleNum]

                # write the article's id, title offset, and block number
                mainListFile.write(id_.to_bytes(4, 'little'))
                mainListFile.write(titleOffset.to_bytes(4, 'little'))
                mainListFile.write(overallBlockNum.to_bytes(4, 'little'))

                articleNum += 1
            overallBlockNum += 1
        
        print("\n      Total articles processed so far:", articleNum)
    
    # close file handles
    mainListFile.close()

def buildBlockList(MAIN_INDEX_PATH):
    '''construct a list of all block locations and write it to disk'''

    # open the file descriptor
    blockListFile = open(MAIN_INDEX_PATH / 'blockList.bin', 'wb')

    blockNum = 0
    nCatalogEntries = len(fileIndex['catalog'])
    for catalogIndex in range(nCatalogEntries): # iterate through each index
        print("  Processing catalog entry {}/{}        ".format(catalogIndex + 1, nCatalogEntries))
        print("    Loading & parsing catalog entry...")

        blocks = catalog_parseIndex(catalogIndex)
        nBlocks = len(blocks)
        blockNum = 0
        
        for block in blocks: # iterate through each block
            blockNum += 1
            print("\r      Working on block {}/{}".format(blockNum, nBlocks), end='')
            
            if block['end'] == -1:
                block['end'] = (DATA_PATH / fileIndex['catalog'][catalogIndex]['xmlFilename']).stat().st_size
            
            # add entry to block list file of the format:
            #   uint32 indexOfCatalogFile
            #   uint32 blockStartLocation
            #   uint32 blockEndLocation
            blockListFile.write(catalogIndex.to_bytes(4, 'little'))
            blockListFile.write(block['start'].to_bytes(4, 'little'))
            blockListFile.write(block['end'].to_bytes(4, 'little'))
        print()
    
    # close file descriptor
    blockListFile.close()

def buildMainIndexes():
    ''' construct all of the main indexes; these allow for quick
    access to article information if you have its title, wikiID,
    or number (sequential indexes for all articles)'''

    # place all files in a folder call "MainIndex";
    # if the indexes are already made, skip rebuilding them
    MAIN_INDEX_PATH = (DATA_PATH / 'MainIndex')
    if MAIN_INDEX_PATH.exists():
        print("Main index already exists (delete 'MainIndex' directory and rerun the program to rebuild)")
        return
    
    MAIN_INDEX_PATH.mkdir()
    
    # build each index file
    print("Building Title Hash Table")
    titleOffsets = buildTitleHashTable(MAIN_INDEX_PATH)
    print("Building ID Hash Table")
    buildIdHashTable(MAIN_INDEX_PATH)
    print("Building Main List")
    buildMainList(MAIN_INDEX_PATH, titleOffsets)
    print("Building Block List")
    buildBlockList(MAIN_INDEX_PATH)

    print('Done building main indexes!')

# build the file indexes and the main indexes
try:
    buildFileIndex()
except:
    print("An error occurred when buidling the file index. Aborting now")
    os._exit(1)
buildMainIndexes()



