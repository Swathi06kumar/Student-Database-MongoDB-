#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pymongo


# In[2]:


from pymongo import MongoClient
from pprint import pprint


# In[3]:


client=MongoClient("mongodb://localhost:27017")


# In[4]:


client.test


# In[5]:


client.list_database_names()


# In[6]:


db=client['Student']


# In[7]:


db.list_collection_names()


# In[8]:


my_collection=db['Student_Database']


# In[9]:


db.list_collections()


# In[10]:


my_collection.find_one()


# In[11]:


for i in my_collection.find():
    pprint(i)
    


# In[12]:


#1)     Find the student name who scored maximum scores in all (exam, quiz and homework)?
stage1={"$unwind":"$scores"}
stage2={"$group":{"_id":"$name","Total_score":{"$sum":"$scores.score"}}}
stage3={"$limit":1}
for i in my_collection.aggregate([stage1,stage2,stage3]):
    pprint(i)


# In[13]:


#2)      Find students who scored below average in the exam and pass mark is 40%?
stage1={"$unwind":"$scores"}
stage2={"$match":{"scores.type":"exam"}}
stage3={"$match":{"scores.score":{"$gte":40}}}
stage4={"$match":{"scores.score":{"$lt":60}}}

for i in my_collection.aggregate([stage1,stage2,stage3,stage4]):
    pprint(i)


# In[14]:


#3)   Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.
stage1={"$unwind":"$scores"}
stage2=({ "$project": {"_id":"$name","score": '$scores.score',"grade": {"$cond": [{"$gte": ['$scores.score', 40]}, 'pass', "fail" ]}}})
for i in my_collection.aggregate([stage1,stage2]):
    pprint(i)


# In[15]:


#4)       Find the total and average of the exam, quiz and homework and store them in a separate collection.
stage1={"$unwind":"$scores"}
stage2={"$group":{"_id":"$scores.type","Total_score":{"$sum":"$scores.score"},"avg_score":{"$avg":"$scores.score"}}}
mycol=db["Grade"]
for i in my_collection.aggregate([stage1,stage2]):
    print(i)
    mycol.insert_one(i)
    


# In[16]:


db.list_collection_names()


# In[17]:


#5)      Create a new collection which consists of students who scored below average and above 40% in all the categories.
stage1={"$unwind":"$scores"}
stage2={"$match":{"scores.score":{"$gte":40}}}
stage3={"$match":{"scores.score":{"$lt":60}}}
stage4={"$project":{"_id":0,"name":1,"scores.score":1,"scores.type":1}}
mycoll=db["Average_students"]

for i in my_collection.aggregate([stage1,stage2,stage3,stage4]):
    pprint(i)
    mycoll.insert_one(i)
    


# In[18]:


db.list_collection_names()


# In[19]:


#6)      Create a new collection which consists of students who scored below the fail mark in all the categories.
stage1={"$unwind":"$scores"}
stage2={"$match":{"scores.score":{"$lt":40}}}
stage3={"$project":{"_id":0,"name":1,"scores.score":1,"scores.type":1}}
mycol2=db["Failures"]

for i in my_collection.aggregate([stage1,stage2,stage3]):
    pprint(i)
    mycol2.insert_one(i)


# In[20]:


db.list_collection_names()


# In[21]:


#7)      Create a new collection which consists of students who scored above pass mark in all the categories.
stage1={"$unwind":"$scores"}
stage2={"$match":{"scores.score":{"$gte":40}}}
stage3={"$project":{"_id":0,"name":1,"scores.score":1,"scores.type":1}}
mycol3=db["passed students"]

for i in my_collection.aggregate([stage1,stage2,stage3]):
    pprint(i)
    mycol3.insert_one(i)


# In[22]:


db.list_collection_names()

