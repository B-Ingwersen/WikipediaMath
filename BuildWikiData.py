# buildWikiData.py
# This program automates of downloading and cleaning all of
# the data needed to create the project's visualizations
#
# WARNING: this program will save ~18 GB of data to disk,
# use almost that much internet bandwidth, and take many
# hours to complete! Use a pre-built data if possible
#
# Author: Bryan Ingwersen
# Date: April 25, 2020

import WikiDataPuller

# display Warning and ask whether to continue
print('''
WARNING: this program will save ~18 GB of data to disk,
use almost that much internet bandwidth, and take many
hours to complete! Use a pre-built data if possible.

The data will be saved to the directory ./data/wikiDump
''')
userAction = input('Are you sure you want to continue? (y/n)')
if userAction == 'y':
    pass
elif userAction == 'n':
    exit()
else:
    print('Unrecognized selection. Aborting now.')
    exit()

# download all the text of wikipedia articles
import WikiDownloader

# index the donwloaded article
import WikiDataIndexer

# build the math index and build additional data sets
mathIndex = WikiDataPuller.SubIndex('Math')
mathIndex.autoBuild('Wikipedia:WikiProject Mathematics/List of mathematics articles ({})')
mathIndex.buildPageRank()
mathIndex.buildNetworkGraphData()
mathIndex.buildViewCatalog()
mathIndex.buildEditCatalog()
mathIndex.buildUserEditCatalog()
mathIndex.buildExtraArticleInfo()
mathIndex.buildExtraUserInfo()
for subIndex in mathIndex.subIndexes.values():
    subIndex.buildPageRank()
    subIndex.buildNetworkGraphData()