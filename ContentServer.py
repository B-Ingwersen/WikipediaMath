# ContentServer.py
# This program generates visualizations of the Wikipedia math article
# data and uses a basic web server to send the content to web page for
# displaying the visualizations in a browser
#
# Author: Bryan Ingwersen
# Date: April 10, 2020

import math
import WikiDataPuller

# data processing/visualization libraries
import plotly.express as px # requires statsmodels for regression
import plotly.graph_objects as go
import pandas as pd
import datetime

# libraries for interacting with the web page visualization
import json
from flask import Flask, request

# INDEX LOADING

loadedIndexes = {} # keep track of loaded indexes so are not
                    # reloaded from disk every time
def loadIndex(name):
    '''Load an index with the WikiDataPuller module'''

    # check if the index was already loaded
    if name in loadedIndexes:
        return loadedIndexes[name]
    
    # load the index and its data accessor system
    index = WikiDataPuller.SubIndex(name, readOnly=True)
    index.setupDataAccessorSystem()

    # save that the index was loaded
    loadedIndexes[name] = index

    # load all subindexes and their requied data sets
    index.loadAllSubIndexes()
    for subIndex in index.subIndexes.values():
        subIndex.loadLinkCatalog()
        subIndex.loadPageRank()
        subIndex.loadNetworkGraphData()
        loadedIndexes[name + '/' + subIndex.name] = subIndex
    
    return index

def loadSubIndex(mainIndexName, subIndexName):
    '''Load a subindex with the WikiDataPuller module'''

    index = loadIndex(mainIndexName)
    subIndex = index.subIndexes[subIndexName]
    return subIndex

# VISUALIZATION BUILDING

def makeNetworkGraph(category):
    '''create a network graph of all the articles of a ceratin category;
    return a dictionary of data sets so that the visualization can be
    constructed in javascript'''

    index = None # the article indexes
    pageRankDistribution = 0.5 # how the bubble sizes are scalled
    links = [] # all the inter-article links
    highlightGroups = [] # set of articles that can be highlighted in groups

    if category is None:
        # load main index
        index = loadIndex('Math')
        pageRankDistribution = 0.2 # scale the bubbles less severely
        
        # enumerate sub indexes so they van be highlighted
        for subIndex in index.subIndexes.values():
            indexes = list(map(lambda x: index.mainDict[x], subIndex.mainList))
            highlightGroups.append([subIndex.name, indexes])   
    else:
        index = loadSubIndex('Math', category)
        links = index.linkCatalog
    
    # set the article bubble sizes based on article page ranks;
    # use a non-linear scaling between 8 and 35 units
    minPageRank = min(index.pageRank) ** pageRankDistribution
    maxPageRank = max(index.pageRank) ** pageRankDistribution
    sizeScale = 27 / (maxPageRank - minPageRank)
    nodeSizes = list(map(lambda x: ( x ** pageRankDistribution - minPageRank) * sizeScale + 8, index.pageRank))

    data = {
        'pointLocs' : index.networkGraphData,
        'pointSizes' : nodeSizes,
        'highlightGroups' : highlightGroups,
        'titles' : index.titles,
        'links' : links
    }

    return data

def makeArticleEditGraph(title):
    '''make a graph of an article's edit history'''

    index = loadIndex('Math')
    articleIndex = index.titles.index(title)

    # load the edit history and the flagged edits
    articleHistory = index.editCatalog[articleIndex]
    flaggedEdits = index.dataAccessor('articles', 'Flagged Edits')[articleIndex]

    # convert the data to use pandas timestamps and editor names
    data = []
    for row in articleHistory:
        data.append([row[0], row[1], pd.to_datetime(row[2], unit='s'), index.users[row[3]] ])
    # set normal edits to blue and flagged edits to red
    colors = ['blue'] * len(data)
    for flagged in flaggedEdits:
        colors[flagged] = 'red'
    # build a datagrame
    df = pd.DataFrame(data, columns=['revisionId', 'size', 'time', 'user'])
    # build the scatter plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['time'],
        y=df['size'],
        marker={'color' : colors},
        line_shape='vh',
        line={'color' : 'darkblue', 'width' : 1},
        hoverinfo='text+x+y',
        hovertext=df['user'],
        mode='lines+markers'
    ))
    fig.update_layout(
        title="Article Edit History",
        xaxis_title="Time",
        yaxis_title="Article Size",
    )

    return fig.to_html(include_plotlyjs=False, full_html=False, include_mathjax=False)

def makeArticleDataGraph(xArg, yArg):
    '''Build a scatter plot of all the articles with selected properties
    (xArg and yArg) on each axis'''
    
    # get the data sets for the axes and the titles
    index = loadIndex('Math')
    xValues = index.dataAccessor('articles', xArg)
    yValues = index.dataAccessor('articles', yArg)
    titles = index.dataAccessor('articles', 'Titles')

    # build a dataframe and plotly graph from it
    df = pd.DataFrame(list(zip(xValues, yValues, titles)), columns=['x', 'y', 'titles'])
    plot = px.scatter(df, x='x', y='y', log_x = True, log_y = True, hover_name='titles',
        labels = {
            'x' : xArg,
            'y' : yArg
        }
    )

    return plot.to_html(include_plotlyjs=False, full_html=False, include_mathjax=False)

def makeEditorDataGraph(xArg, yArg):
    '''Build a scatter plot of all the editors with selected properties
    (xArg and yArg) on each axis'''

    index = loadIndex('Math')

    # get the data sets for the axes and the titles
    xValues = index.dataAccessor('editors', xArg)
    yValues = index.dataAccessor('editors', yArg)
    names = index.dataAccessor('editors', 'Names')

    # build a dataframe and plotly graph from it
    df = pd.DataFrame(list(zip(xValues, yValues, names)), columns=['x', 'y', 'names'])
    plot = px.scatter(df, x='x', y='y', log_x = True, log_y = True, hover_name='names',
        labels = {
            'x' : xArg,
            'y' : yArg
        }
    )

    return plot.to_html(include_plotlyjs=False, full_html=False, include_mathjax=False)

def makePageViewGraph(views):
    '''build a graph of page views using the list views passed to it'''

    # calculate the month associated with each view count and create
    # a list of dictionaries with the data
    viewData = []
    date = datetime.date(2020, 3, 1)
    for view in reversed(views):
        viewData.append({
            'date' : pd.to_datetime(date),
            'views' : view
        })
        date -= datetime.timedelta(days=1)
        date = date.replace(day=1)
    
    viewData.reverse() # reverse the direction to go forward in time

    # build a dataframe and plot the data
    df = pd.DataFrame(viewData)
    plot = px.line(df, x='date', y='views', title='Page View History',
        labels = {
            'date' : 'Month',
            'views' : 'Monthly Page Views'
        },
    )
    
    return plot.to_html(include_plotlyjs=False, full_html=False, include_mathjax=False)

def makeArticlePageViewGraph(title):
    '''make a page view graph for a single article'''

    # extract the article's page view data
    index = loadIndex('Math')
    articleIndex = index.titles.index(title)
    pageViews = index.dataAccessor('articles', 'Monthly Page Views')[articleIndex]

    return makePageViewGraph(pageViews)

def makeCategoryPageViewGraph(category=None):
    '''make a page view graph for a category of articles'''

    # load page view data
    pageViewData = loadIndex('Math').categoryDataAccessor('Monthly Page Views', category=category)

    pageViews = [0 for _ in range(57)] # represents 57 months of page views going backwards
    # combine page views for every article in the category
    for views in pageViewData:
        for i, view in enumerate(reversed(views)):
            pageViews[i] += view
    
    pageViews.reverse() # make pageViews go forward in time
    return makePageViewGraph(pageViews)

def getCategoryInfo(category):
    '''return a dictionary of information about a category'''

    index = loadIndex('Math')
    
    numEditors = len(index.users) # get the total number of editors for the index
    if category is not None:
        # calculate the total number of editors for the category
        editors = []
        for articleEditors in index.categoryDataAccessor('Editors', category):
            editors.extend(articleEditors)
        numEditors = len(set(editors)) # remove duplicates
    
    # calculate additional statistics aggregated for the category
    SRI = index.categoryDataAccessor('Student Reference Index', category)
    AricleViews = index.categoryDataAccessor('Monthly Page Views', category)
    weightedSRI = 0
    totalViews = 0
    for i in range(len(index.categoryDataAccessor('Article Numbers', category))):
        nViews = sum(AricleViews[i])
        totalViews += nViews
        weightedSRI += SRI[i] * nViews
    weightedSRI /= totalViews

    info = {
        'numArticles' : len(index.categoryDataAccessor('Article Numbers', category)),
        'numEdits' : sum(index.categoryDataAccessor('Number of Edits', category)),
        'flaggedEdits' : sum(index.categoryDataAccessor('Number of Flagged Edits', category)),
        'totalSize' : sum(index.categoryDataAccessor('Article Size', category)),
        'numEditors' : numEditors,
        'totalViews' : totalViews,
        'weightedSRI' : weightedSRI,
    }

    return info

def getArticleInfo(title):
    '''return a dictionary of information about an article with given title'''

    index = loadIndex('Math')
    articleIndex = index.titles.index(title)

    info = {
        'numOutLinks' : index.articleDataAccessor('Number of Outgoing Links', articleIndex),
        'numInLinks' : index.articleDataAccessor('Number of Incoming Links', articleIndex),
        'numEdits' : index.articleDataAccessor('Number of Edits', articleIndex),
        'numFlaggedEdits' : index.articleDataAccessor('Number of Flagged Edits', articleIndex),
        'numViews' : sum(index.articleDataAccessor('Monthly Page Views', articleIndex)),
        'numEditors' : index.articleDataAccessor('Number of Editors', articleIndex),
        'sri' : index.articleDataAccessor('Student Reference Index', articleIndex),
        'pageRank' : index.articleDataAccessor('Page Rank', articleIndex),
        'size' : index.articleDataAccessor('Article Size', articleIndex),
    }
    return info

# WEB SERVER FUNCITONALITY

# create the web server
app = Flask(__name__)

@app.route('/')
def serveMainPage():
    html = ''
    with open('Visualization.html') as mainPage:
        html = mainPage.read()
    return html

@app.route('/NetworkDiagram')
def serveNetworkDiagram():
    category = request.args.get('category')
    return json.dumps(makeNetworkGraph(category))

@app.route('/PageViews')
def servePageViews():
    article = request.args.get('article')
    if article is not None:
        return makeArticlePageViewGraph(article)
    else:
        category = request.args.get('category')
        return makeCategoryPageViewGraph(category=category)

@app.route('/EditHistory')
def serveEditHistory():
    article = request.args.get('article')
    if article is not None:
        return makeArticleEditGraph(article)

@app.route('/ArticleData')
def serveArticleData():
    xAxis = request.args.get('xAxis')
    yAxis = request.args.get('yAxis')

    if xAxis is None or yAxis is None:
        return ''

    return makeArticleDataGraph(xAxis, yAxis)

@app.route('/EditorData')
def serveEditorData():
    xAxis = request.args.get('xAxis')
    yAxis = request.args.get('yAxis')

    if xAxis is None or yAxis is None:
        return ''

    return makeEditorDataGraph(xAxis, yAxis)

@app.route('/ArticleInfo')
def serveArticleInfo():
    article = request.args.get('article')
    
    return json.dumps(getArticleInfo(article))

@app.route('/CategoryInfo')
def serveCategoryInfo():
    category = request.args.get('category')
    return json.dumps(getCategoryInfo(category))

# run the Flask web server
if __name__ == '__main__':
   app.run()