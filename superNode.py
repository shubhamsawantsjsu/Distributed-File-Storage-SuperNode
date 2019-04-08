import sys  
sys.path.append('./proto')
from concurrent import futures
from threading import Thread
import grpc
    
import db
import fileService_pb2_grpc
import fileService_pb2
import time
import threading
from ClusterStatus import ClusterStatus


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class FileServer(fileService_pb2_grpc.FileserviceServicer):
    def __init__(self, hostIP, port):
        self.serverAddress = hostIP+":"+port
        self.clusterLeaders = {}
        self.clusterStatus = ClusterStatus()
        self.ip_channel_dict = {}

    def getLeaderInfo(self, request, context):
        print("getLeaderInfo Called")
        address = request.ip + ":" + request.port
        self.clusterLeaders[request.clusterName] = address
        print("ClusterLeaders: ",self.clusterLeaders)
        channel = grpc.insecure_channel('{}'.format(address))
        self.ip_channel_dict[address] = channel
        return fileService_pb2.ack(success=True, message="Leader Updated.")

    def UploadFile(self, request_iterator, context):
        print("Inside Server method ---------- UploadFile")
        
        node, node_replica, clusterName, clusterReplica = self.clusterStatus.leastUtilizedNode(self.clusterLeaders)
       
        if(node==-1):
            return fileService_pb2.ack(success=False, message="No Active Clusters.")
        
        print("Node found is:", node)

        channel1 = self.ip_channel_dict[node]
        stub1 = fileService_pb2_grpc.FileserviceStub(channel1)
        if(node_replica!="" and node_replica in self.ip_channel_dict):
            channel2 = self.ip_channel_dict[node_replica]
            stub2 = fileService_pb2_grpc.FileserviceStub(channel2)
        else: stub2 = None
        
        filename, username = "",""
        data = bytes("",'utf-8')
        
        for request in request_iterator:
            filename, username = request.filename, request.username
            break
        
        if(self.fileExists(username, filename)):
            return fileService_pb2.ack(success=False, message="File already exists for this user. Please rename or delete file first.")


        def sendDataStreaming(username, filename, dataChunk):
            #data+=dataChunk
            yield fileService_pb2.FileData(username=username, filename=filename, data=dataChunk)
            for request in request_iterator:
                #data+=request.data
                yield fileService_pb2.FileData(username=request.username, filename=request.filename, data=request.data)
        
        resp1 = stub1.UploadFile(sendDataStreaming(username, filename, request.data))
        
        # if(stub2 is not None):
        #     t1 = Thread(target=self.replicateData, args=(stub2,username,filename,data,))
        #     t1.start()

        if(resp1.success):
            db.saveMetaData(username, filename, clusterName, clusterReplica)
        
        return resp1

    def DownloadFile(self, request, context):
        fileMeta = db.parseMetaData(request.username, request.filename)
        
        primaryIP, replicaIP = -1,-1
        channel1, channel2 = -1,-1
        if(fileMeta[0] in self.clusterLeaders): 
            primaryIP = self.clusterLeaders[fileMeta[0]]
            channel1 = self.clusterStatus.isChannelAlive(primaryIP)
            
        if(fileMeta[1] in self.clusterLeaders):
            replicaIP = self.clusterLeaders[fileMeta[1]]
            channel2 = self.clusterStatus.isChannelAlive(replicaIP)

        if(channel1):
            stub = fileService_pb2_grpc.FileserviceStub(channel1)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
            for response in responses:
                yield response
        elif(channel2):
            stub = fileService_pb2_grpc.FileserviceStub(channel2)
            responses = stub.DownloadFile(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
            for response in responses:
                yield response
        else:
            return fileService_pb2.FileData(username=request.username, filename=request.filename, data=bytes("",'utf-8'))

    def fileExists(self,username, filename):
        return db.keyExists(username + "_" + filename)


    def replicateData(self,stub, username, filename, data):
        
        def streamData(username, filename, data):
            chunk_size = 4000000
            start, end = 0, chunk_size
            while(True):
                chunk = data[start:end]
                if(len(chunk)==0): break
                start=end
                end += chunk_size
                yield fileService_pb2.FileData(username=username, filename=filename, data=chunk)
            
        resp = stub.UploadFile(streamData(username,filename,data))

def FileDelete(self, request, data):
    fileMeta = db.parseMetaData(request.username, request.filename)
    
    primaryIP, replicaIP = -1,-1
    channel1, channel2 = -1,-1
    if(fileMeta[0] in self.clusterLeaders): 
        primaryIP = self.clusterLeaders[fileMeta[0]]
        channel1 = self.clusterStatus.isChannelAlive(primaryIP)
        
    if(fileMeta[1] in self.clusterLeaders):
        replicaIP = self.clusterLeaders[fileMeta[1]]
        channel2 = self.clusterStatus.isChannelAlive(replicaIP)

    if(channel1):
        stub = fileService_pb2_grpc.FileserviceStub(channel1)
        response = stub.FileDelete(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        
        if(response.success==True):
            return fileService_pb2.ack(success=True message="File successfully deleted from cluster : ", fileMeta[0])
        else:
            return fileService_pb2.ack(success=False message="Internal error")
    
    if(channel2):
        stub = fileService_pb2_grpc.FileserviceStub(channel2)
        response = stub.FileDelete(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        
        if(response.success==True):
            return fileService_pb2.ack(success=True message="File successfully deleted from cluster : ", fileMeta[0])
        else:
            return fileService_pb2.ack(success=False message="Internal error")
        
        print(response.message)


def FileSearch(self, request, data):
    fileMeta = db.parseMetaData(request.username, request.filename)

    primaryIP, replicaIP = -1,-1
    channel1, channel2 = -1,-1

    if(fileMeta[0] in self.clusterLeaders): 
        primaryIP = self.clusterLeaders[fileMeta[0]]
        channel1 = self.clusterStatus.isChannelAlive(primaryIP)
        
    if(fileMeta[1] in self.clusterLeaders):
        replicaIP = self.clusterLeaders[fileMeta[1]]
        channel2 = self.clusterStatus.isChannelAlive(replicaIP)

    if(channel1):
        stub = fileService_pb2_grpc.FileserviceStub(channel1)
        response = stub.FileSearch(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        if(response.success==True):
            return fileService_pb2.ack(success=True, message="File exists in the cluster : ", fileMeta[0])

    elif(channel2):
        stub = fileService_pb2_grpc.FileserviceStub(channel2)
        response = stub.FileSearch(fileService_pb2.FileInfo(username = request.username, filename = request.filename))
        if(response.success==True):
            return fileService_pb2.ack(success=True, message="File exists in the replica cluster : ", fileMeta[1])
    
    return fileService_pb2.ack(success=False, message="File does not exist in any cluster.")

    


def run_server(hostIP, port):
    print('Supernode started on {}:{}'.format(hostIP, port))

    #GRPC 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fileService_pb2_grpc.add_FileserviceServicer_to_server(FileServer(hostIP, port), server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

# ----------------------Main-------------------- #
if __name__ == '__main__':
    hostIP = "localhost"
    port = "9000"
    run_server(hostIP, port)