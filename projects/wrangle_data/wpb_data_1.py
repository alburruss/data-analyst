# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 18:57:18 2016

@author: andrew

UdC Project 3

mongod session and queries of wpb data

raw file
mng_wpb_pmp.osm.json

db
wpb

collection
wpbMap

# import command
mongoimport --db wpb --collection wpbMap --drop --file mng_wpb_pmp.osm.json

"""

import pprint
import pymongo

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

# MongoClient
from pymongo import MongoClient
# explicitly specify host and port
client = MongoClient('localhost:27017')
# get a database
# db name
db = client.wpb


'''
# need collection name
# need to specify args for findone
# pprint to logFile returns shallow top level keys
# this cursor will test if db session is up
cursor0 = db.wpbMap.find_one({"id":"95948156"})

# pprint iteration: see note below
# match on first level keys

cursor5 = db.wpbMap.find_one({"type": "node"})

cursor6 = db.wpbMap.find_one({"type": "way"})



# 2nd level key
cursor7 = db.wpbMap.find_one({"created.user": "grouper" })

# create index


# top ten aggregation pipeline
# users contributing "type": "node"

cursor10 = db.wpbMap.aggregate([ {"$match": {"type":"node"}}, 
                                  {"$group": {"_id": "$created.user", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 20}
                                  ])

############################################################ match "type": "nd" 

# users contributing "type": "way"

cursor12 = db.wpbMap.aggregate([ {"$match": {"type":"way"}}, 
                                  {"$group": {"_id": "$created.user", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 20}
                                  ])

# users contributing any type
cursor11 = db.wpbMap.aggregate([ {"$group": {"_id": "$created.user", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 20}
                                  ])

#cursor21 = db.wpbMap.find({"address.street": "Plaza Real" })

# only returns exact match
# cursor22 = db.wpbMap.find({"address.street": "Plaza" })

# $text $search is not working: not supported in version ??
########################################################### try 2nd level "k"

# try text search in agg pipeline
'''

#cursor28 = db.wpbMap.find({"address.street": '1515 Perimeter Rd, West Palm Beach, FL 33406' })

#cursor29 = db.wpbMap.find( {"address.street" :  "324 Datura St #207"} )



# match on unique id 
# correct both address fields
result_1 = db.wpbMap.update_one(
    {u'id': u'2572628284'},
    {
        "$set": {
            "addr:street" :  "Datura St",
            "addr:housenumber" : "324",
            "addr:suitenumber" : "207",
            "address.street" :  "Datura St",
            "address.housenumber" : "324",
            "address.suitenumber" : "207"
        },
        "$currentDate": {"updated" : True}
    }, upsert = True
)

print result_1.matched_count
print result_1.modified_count

cursor29 = db.wpbMap.find( {u'id': u'2572628284'} )
'''


cursor25 = db.wpbMap.aggregate([ {"$match": {"type":"nd"}}, 
                                    { "$limit": 20}
                                  ])


cursor26 = db.wpbMap.find_one({"type": "relation"})


#print cursor0


# agg pipeline: most frequent city
cursor31 = db.wpbMap.aggregate([ {"$group": {"_id": "$address.city", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 40}
                                  ])

# agg pipeline: most frequent street
cursor32 = db.wpbMap.aggregate([ {"$group": {"_id": "$address.street", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 200}
                                  ])                                  


# agg pipeline: most frequent phone
cursor33 = db.wpbMap.aggregate([ {"$group": {"_id": "$phone", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 200}
                                  ])

# agg pipeline: most frequent www url
cursor34 = db.wpbMap.aggregate([ {"$group": {"_id": "$website", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 200}
                                  ])

# agg pipeline: most frequent name
cursor35 = db.wpbMap.aggregate([ {"$group": {"_id": "$name", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 200}
                                  ])


cursor23 = db.wpbMap.aggregate([ {"$match": {"address.street": "S Dixie Highway"} }, 
                                    { "$limit": 20}
                                  ])


cursor24 = db.wpbMap.aggregate([ {"$match": {"payment:bitcoin": "yes"} }, 
                                    { "$limit": 20}
                                  ])

#waterway queries
cursor25 = db.wpbMap.aggregate([ {"$match": {"name": "L-18"} }, 
                                    { "$limit": 20}
                                  ])
                                  
cursor26 = db.wpbMap.aggregate([ {"$match": {"waterway": "canal"} }, 
                                    { "$limit": 20}
                                  ]) 

cursor27 = db.wpbMap.aggregate([ {"$match": {"waterway": "weir"} }, 
                                    { "$limit": 20}
                                  ]) 

# agg pipeline: waterways
cursor36 = db.wpbMap.aggregate([ {"$group": {"_id": "$waterway", "count": {"$sum": 1}} },
                                    {"$sort": {"count": -1} },
                                    { "$limit": 200}
                                  ])
'''

# print to log file
logFile = open('query_wpb_38'+'.log', 'w')

pprint.pprint( '' , logFile )
pprint.pprint( 'query: address.street err ' , logFile )
pprint.pprint( '' , logFile )

for document in cursor29: 
    pprint.pprint(document, logFile)


'''
pprint.pprint( '' , logFile )
pprint.pprint( 'query: name: L-18 ' , logFile )
pprint.pprint( '' , logFile )

for document in cursor25: 
    pprint.pprint(document, logFile)    


pprint.pprint( '' , logFile )
pprint.pprint( 'query: waterway: canal' , logFile )
pprint.pprint( '' , logFile )

for document in cursor26: 
    pprint.pprint(document, logFile)    

pprint.pprint( '' , logFile )
pprint.pprint( 'query: waterway: weir' , logFile )
pprint.pprint( '' , logFile )

for document in cursor27: 
    pprint.pprint(document, logFile)
'''

pprint.pprint( '' , logFile )
pprint.pprint( 'end of query' , logFile )    
pprint.pprint( '' , logFile )    

logFile.close() 
