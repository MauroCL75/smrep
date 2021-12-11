import pymongo
import datetime
import logging

from pymongo.write_concern import WriteConcern

def yield_rows(cursor, chunk_size=1000):
    """
    Generator to yield chunks from cursor
    :param cursor:
    :param chunk_size:
    :return:
    """
    chunk = []
    logging.info("yr")
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk

class MyProc:
    '''A connection'''
    iteration=0
    last={} #Last processed row
    operation=None #current operation
    src=None #source cursor
    collections=[] #collections from source
    dest=None #destination cursor
    keyword=None #keyworkd to check date
    removeId=False # do I remove ids?

    def __init__(self, data, mech='SCRAM-SHA-256'):
        logging.info("init")
        self.operation="initialization"
        if "login" in data["source"].keys():
            connSource = pymongo.MongoClient(host=data["source"]["host"], port=27017,
                username=data["source"]["login"], password=data["source"]["password"], 
                authSource=data["source"]["database"], authMechanism=mech)
        else:
            connSource = pymongo.MongoClient(host=data["source"]["host"])
        if "login" in data["destination"].keys():
            connDest = pymongo.MongoClient(host=data["destination"]["host"], port=27017,
                username=data["destination"]["login"], password=data["destination"]["password"], 
                authSource=data["destination"]["database"], authMechanism=mech)
        else: 
            connDest = pymongo.MongoClient(host=data["destination"]["host"])
        self.src = connSource[data["source"]["database"]]
        self.dest = connDest[data["destination"]["database"]].with_options(write_concern=WriteConcern(w=0))
        self.collections = data["source"]["collections"]
        for i in self.collections:
            self.last[i] = None
        self.keyword = data["source"]["keyword"]
        self.removeId = False
        if ("removeId" in data["destination"]):
            if (data["destination"]["removeId"]=="true"):
                self.removeId = True 

    def refresh(self, agodays = 1, chsize = 1000):
        '''Get new data since last filter'''
        logging.info("refresh start")
        self.operation="refresh"
 
        
        for collection in self.collections:
            logging.info("collection")
            if self.last[collection] != None:
                filter = {self.keyword: {'$gt': self.last[collection] }}
            else:
                today = datetime.datetime.utcnow() - datetime.timedelta(days = agodays)
            filter = {self.keyword: {"$gt": today }}
            logging.info("%s %s",collection, filter)
            cnt = self.src[collection].count(filter)
            logging.info("%s src: %i",collection, cnt)
            cursor = self.src[collection].find(filter)
            logging.info("%s cursor created",collection)
            tmp = []
            n = 1
            for chunk in yield_rows(cursor, chsize):
                logging.info("len chunk: %i", len(chunk))
                for i in chunk:
                    if self.removeId:
                        i.pop("_id",None)
                    tmp.append(i)
                    n = n+1
                    ###also it could be a queue here
                logging.info("%s chunk %i", collection, n)
                if len(tmp)>0:
                    self.dest[collection].insert_many(tmp)
                    logging.info("inserted in dest %s %i", collection, len(tmp))
                    tmp = []
            max = self.dest[collection].find_one(sort=[(self.keyword,-1)])
            if max!=None:
                self.last[collection] = max[self.keyword]
                logging.info("%s new max %s", collection, max[self.keyword])
        self.iteration += 1
        logging.info("refresh done!")
