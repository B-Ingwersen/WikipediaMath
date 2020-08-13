Math Wikipedia Articles Analysis
Author: Bryan Ingwersen

OVERVIEW:
    The Math Wikipedia Articles Analysis project is a tool for visualizing data
of the approximately 23000 math articles on English Wikipedia. It creates
various graphs showing connections between articles, article categories, view
history, and edit history. This file provides general information about running 
setting up and runningthe visualization and rebuilding the underlying data.

SETUP:
    The visualization requires the following files in the same directory:
        WikiDataPuller.py
        ContentServer.py
        Visualization.html
        WikiCategoryScraper.py
        ThreadPool.py

    Rebuilding the underlying data also requires the following files:
        BuildWikiData.py
        WikiDownloader.py
        WikiDataIndexer.py
        forceDirectedGraphMaker.cpp

    Additionally the following python packages should be installed (these should
    be available through pip3):
        bs4 (contains BeautifulSoup)
        plotly (graphing library)
        pandas (data management)
        flask (lightweight web server)
    
    The visualization data should be placed in the relative path:
        data/wikiDump/SubIndexes/Math
    
    If you are rebuliding the data, the following empty folders should be made:
        data/wikiDump
        data/wikiDump/SubIndexes

VISUALIZATION:
    The visualization is run from the python file ContentServer.py (python3 is
required; python2 will not work). Running this will start a tiny web server on
a localhost port; normally this will be localhost:5000, but check the program's
output to be sure. Open up this link in your browser to view the visualization.
When you first start the visualization, it might take a few seconds to load.
You can then explore the pieces of the visualization while ContentServer.py is
still running. Note that closing the browser will not kill the webserver; kill
ContentServer.py from the environment it is running in to fully close the
program; if it's running from a terminal, check for the key combination that
flask says will quit the program (Ctrl-C on most systems).

REBUILDING DATA:
    The underlying visualization data sets can be rebuilt with the script
BuildWikiData.py. HOWEVER, this program will download all the compressed text
of English Wikipedia, occupy about 18 GB on disk, and take many hours to
complete. If possible, it is recommended that you use a copy of the math article
visualization data that is prebuilt (which is only about 70 MB).
    If you want to rebuild it, first the program forceDirectedGraphMaker.cpp
must be compiled (this file might have to be modified in order to compile on
your platform; it has only been tested on a 64-bit linux distribution). Then,
the method 'buildNetworkGraphData' of class 'SubIndex' of the file
WikiDataPuller.py must be modified in order to call the program 
forceDirectedGraphMaker; a commented out line in the function includes code that
should work only unix systems if the executable is in the same directory as the
python script. After this, running the python script BuildWikiData.py should
download and rebuild all the required data. Note that this process will probably
take many hours.

        
