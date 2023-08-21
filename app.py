from dotenv import load_dotenv, find_dotenv
import os, pprint
from pymongo import MongoClient
from datetime import datetime as dt

load_dotenv(find_dotenv())
pwd = os.environ.get("MONGO_PWD")

printer = pprint.PrettyPrinter()

conn = f"mongodb+srv://mornav:{pwd}@cluster0.tam2mur.mongodb.net/?retryWrites=true&w=majority&authSource=admin"

client = MongoClient(conn)

dbs = client.list_database_names()
testdb = client['test01']

# coll = testdb.list_collection_names()
# print(coll)

def insert_01():
    contacts_coll = testdb.contacts
    test_data = {
        "name": "Arnav",
        "age": 19,
    }
    insert_id = contacts_coll.insert_one(test_data).inserted_id
    print(insert_id)

# insert_01()

def insert_02():
    prod_coll = testdb.prod
    names = ['Arnav','Nihar','Anup','Sarthak','Soham']
    age = [13,12,43,12,3,41]
    height = [232,343,123,123,123]

    docs = []

    for n,a,h in zip(names,age,height):
        data = {"name":n,"age":a,"height":h}
        docs.append(data)
    
    ok = prod_coll.insert_many(docs)
    print(ok)

# insert_02()

def read_01():
    prod_coll = testdb.prod
    data = prod_coll.find_one({"name":"Arnav"})
    print(data)

# read_01()

def read_02():
    prod_coll = testdb.prod
    count = prod_coll.count_documents({"age":3})
    print(count)

# read_02()

from bson.objectid import ObjectId
def read_03(name_id):
    _id = ObjectId(name_id)
    prod_coll = testdb.prod
    data = prod_coll.find({"_id":_id})
    printer.pprint(data)

# read_03('64e1c5ebe57052ba182ece2e')

def read_04(min_age,max_age):
    query = {
        "$and":[
            {"age": { "$gte": min_age}},
            {"age": {"$lte": max_age}},
        ]

    }
    prod_coll = testdb.prod
    data = prod_coll.find(query).sort("age")

    for i in data:
        printer.pprint(i)

    

# read_04(12,56)

def read_05():
    prod_coll = testdb.prod
    prod_col = {"_id":0,"name":1,"height":1}
    data = prod_coll.find({},prod_col)
    for i in data:
        printer.pprint(i)

# read_05()

def update_01(per_id):
    _id = ObjectId(per_id)
    prod_coll = testdb.prod
    
    all_updates = {
        "$set":{"new_field":True},
        "$inc":{"age":189},
        "$rename":{"height":"Ht"},
        "$unset":{"new_field":""},

    }

    data = prod_coll.update_one({"_id":_id}, all_updates)
    print(data)

# update_01('64e1c5ebe57052ba182ece2f')

def update_02(per_id):
    _id = ObjectId(per_id)

    prod_coll = testdb.prod
    

    doc = {
        
        "name":"Anup the Man",
        "age":24,
    }
    prod_coll.replace_one({"_id":_id},doc)

#update_02('64e1c5ebe57052ba182ece2f')

def delete_01(per_id):
    _id = ObjectId(per_id)

    prod_coll = testdb.prod
    prod_coll.delete_one({"_id":_id})

address = {
    "City":"Mumbai",
    "Street":"Mountain Avenue",
    "Block":"CA/10",
}

def add_address_01(per_id,address):
    _id = ObjectId(per_id)

    prod_coll = testdb.prod
    prod_coll.update_one({"_id":_id},{"$addToSet":{'addresses':address}})

# add_address_01('64e1c5ebe57052ba182ece2f',address)

def add_address_02(per_id,address):
    _id = ObjectId(per_id)

    address = address.copy()
    address['owner_id'] = per_id
    add_coll = testdb.addresses
    add_coll.insert_one(address)

# add_address_02('64e1c5ebe57052ba182ece30',address)

validate = {
    "$jsonSchema" : {
        "bsonType": 'object',
        "required" : [ 'name', 'year', 'major', 'address' ],
        "properties": {
        "name": {
            "bsonType": 'string',
            "description": 'must be a string and is required'
      },
      "authors": {
          "bsonType":'array',
          "items":{
              'bsonType': 'objectId',
              'description':'must be an objectid and its required'
          }
      },
      "year": {
        "bsonType": 'int',
        "minimum": 2017,
        "maximum": 3017,
        "description": 'must be an integer in [ 2017, 3017 ] and is required'
      },
      "gpa": {
        "bsonType": [ 'double' ],
        "description": 'must be a double if the field exists'
      }

    }
}
}

try:
    testdb.create_collection('info')
except Exception as e:
    print(e)

    testdb.command('collMod',"info",validator=validate)

# def create_auth():
#     prod_coll = testdb.prod
#     data = prod_coll.find({})
#     for peep in data:
#         print(peep['_id'])

#     info = [
#         {
#             "name":"Parsh",
#             "year":2018.
#             ""
#         }
#     ]

# create_auth()   













