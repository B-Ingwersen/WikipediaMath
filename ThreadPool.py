# ThreadPool.py
# Define a class for creating a basic thread pool; use to
# parallelize API requests
#
# Author: Bryan Ingwersen
# Date: April 5, 2020

import threading
import time

class ThreadPool:
    def __init__(self, func, data={}, nThreads=8):
        '''Create a thread pool with a run function
        and data object accessible to all'''

        # default lock for accessing data variables
        data['lock'] = threading.Lock()

        self.data = data
        self.func = func
        self.nThreads = nThreads

        # create nThreads
        self.threads = [threading.Thread(target=self.runFunc) for i in range(nThreads)]
    
    def start(self):
        ''' start all the threads in the thread pool'''
        for thread in self.threads:
            thread.start()
    
    def waitAll(self):
        ''' wait for all threads to finish '''
        for thread in self.threads:
            thread.join()
    
    def runFunc(self):
        ''' function to start a thread's execution '''
        self.func(self.data)
