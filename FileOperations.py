from concurrent import futures
import sys
import psutil
import threading
import grpc
import time
sys.path.append('./Gen')
import fileService_pb2
import fileService_pb2_grpc
import heartbeat_pb2_grpc
import heartbeat_pb2
import yaml
import threading
import hashlib
from activeNodes import activeNodes
from nodeSelect import nodeSelect
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


chunk_id=0

class FileService(fileService_pb2_grpc.FileserviceServicer):
	def __init__(self, leader, serverAddress,activeNodeObj):
		self.leader= leader
		self.serverAddress= serverAddress
		self.nodeSelect= nodeSelect()
		self.activeNodeObj=activeNodeObj

	def UploadFile(self, request_iterator, context):
		global chunk_id
		print('here', self.leader)

		if self.leader:
			print("I am the leader")
			chunk_id=0
			for chunk in request_iterator:
				username= chunk.username
				filename = chunk.filename
				destination= self.nodeSelect.leastUtilizedNode(self.serverAddress)
				print(destination, self.serverAddress)
				if destination==9999:
					return fileService_pb2.ack(success=False, message="No active nodes!")
				if destination==self.serverAddress:
					print("data stored on primary")
					##Metdata broadcast
					##Mongodb store
				else:
					child_response = self.sendDataToDestination(chunk, destination)
					if not child_response.success:
						return fileService_pb2.ack(success=False, message="Error saving chunk at: " + destination)
					# chunk_id+=1
					##Metdata broadcast
			return fileService_pb2.ack(success=True, message="Data has been saved!")
		else:
			print("I am NOT the leader")
			for request in request_iterator:
				print("data stored on"+request.username)
				##mongodb save data
			return fileService_pb2.ack(success=True, message="Data has been saved at " + self.serverAddress)

	 			
	def sendDataToDestination(self, chunk, node):
		self.activeNodeObj.channelRefresh()
		print(self.activeNodeObj.getActiveIpsDict()[node])
		channel= self.activeNodeObj.getActiveIpsDict()[node]
		stub = fileService_pb2_grpc.FileserviceStub(channel)
		print('Calling next IP', self.activeNodeObj.isChannelAlive(channel))
		return stub.UploadFile(self.dummy_generator(chunk))

	def dummy_generator(self, chunk):
		yield fileService_pb2.FileData(username=chunk.username, filename=chunk.filename,data=chunk.data)







# def serve():
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     heartbeat_pb2_grpc.add_HearBeatServicer_to_server(Heartbeat(), server)
#     server.add_insecure_port('[::]:50051')
#     server.start()
#     try:
#         while True:
#             time.sleep(_ONE_DAY_IN_SECONDS)
#     except KeyboardInterrupt:
#         server.stop(0)

# def client():
# 	with grpc.insecure_channel('localhost:50051') as channel:
# 		stub = heartbeat_pb2_grpc.HearBeatStub(channel)
# 		response = stub.isAlive(heartbeat_pb2.NodeInfo())
# 		print("Greeter client received: " + response.used_mem)
		

# if __name__ == '__main__':
#     t1 = threading.Thread(target=serve)
#     t2 = threading.Thread(target=client)
#     t1.start()
#     time.sleep(2)
#     t2.start()
#     t1.join()
#     t2.join()
