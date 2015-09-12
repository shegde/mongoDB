"""
Script to create 1000s of connections to MongoDB Master node
Uses multiple processes to create these connections in parallel

Max connections to a single Mongo is 16000 (mongo cli db.serverStatus().connections())

# other notes and useful links:
# this script has been tested only on MACs
# if resources are not sufficient on MAC refer to: http://superuser.com/questions/433746/is-there-a-fix-for-the-too-many-open-files-in-system-error-on-os-x-10-7-1
# https://docs.python.org/2/library/multiprocessing.html
# http://api.mongodb.org/python/current/api/pymongo/mongo_client.html
# http://api.mongodb.org/python/current/tutorial.html
# http://docs.mongodb.org/manual/reference/mongo-shell/
# http://www.quantstart.com/articles/Parallelising-Python-with-Threading-and-Multiprocessing

"""

import time, os
from pymongo import MongoClient
from multiprocessing import Process

connections = []

# number of connections to DB per process
numConnections = 785

# number of processes to be run in parallel
numProcs = 20

# numConnections * numProcs = total connections to be created
# This is the number shown by
# db.command("serverStatus")['connections']['available'] before running this script

# hold the connections for this duration in secs
sleepTime = 300

# mongodb connection details
mongoServer = '10.5.107.213'
mongoPort = 27017

# create connections
def create():
    pid = os.getpid()
    print "OPENING %s CONNECTIONS FROM PID %s" % (numConnections, pid)
    for n in range(1, numConnections+1):
        if n % 500 == 0 and n != 0:
            print 'PID %s CREATED %s' % (pid, str(n))
        client = MongoClient(mongoServer, mongoPort)
        connections.append(client)
    getAvailableConnections(client)
    print "%s WAITING FOR %s SECS" % (pid, sleepTime)
    time.sleep(sleepTime)

# retrieve available connections from mongo master
def getAvailableConnections(c):
    db = c.chat # go to any database like chat, indigo etc
    print 'CONNECTIONS AVAILABLE: ', db.command("serverStatus")['connections']['available']

if __name__ == '__main__':
    jobs = []
    for i in range(0, numProcs):
        p = Process(target=create)
        jobs.append(p)
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
    print "DONE"


