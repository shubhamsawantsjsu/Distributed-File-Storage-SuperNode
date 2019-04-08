import redis
import ast

_redis_port = 6379

r = redis.StrictRedis(host='localhost', port=_redis_port, db=0)

#metadata = {"username_filename" : [clusterName, clusterReplica]}
def saveMetaData(username, filename, clusterName, clusterReplica):
    key = username + "_" + filename
    r.set(key,str([clusterName,clusterReplica]))

def parseMetaData(username, filename):
    key = username + "_" + filename
    return ast.literal_eval(r.get(key).decode('utf-8'))

def keyExists(key):
    return r.exists(key)


