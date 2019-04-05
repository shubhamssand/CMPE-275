from concurrent import futures
import sys
import psutil
import threading
import grpc
import time
from activeNodes import activeNodes
sys.path.append('./Gen')
import heartbeat_pb2
import heartbeat_pb2_grpc
_ONE_DAY_IN_SECONDS = 60 * 60 * 24

activeNodeObj = activeNodes()
class nodeSelect():
	def __init__(self):
		activeNodeObj.channelRefresh()
		self.activeIpList = activeNodeObj.getActiveIpsDict()
		print(self.activeIpList)

	def leastUtilizedNode(self, serverAddress):
		min= float("-inf")
		dummy= []
		for ip, channel in self.activeIpList.items():
			if ip == serverAddress:
				continue
			stub = heartbeat_pb2_grpc.HearBeatStub(channel)
			response = stub.isAlive(heartbeat_pb2.NodeInfo())
			dummy.append((float(response.cpu_usage), float(response.disk_space), float(response.used_mem), ip, channel))
		if len(dummy)==0:
			return 9999
		print("dummy", dummy)
		a=sorted(dummy,key=lambda x: (x[1]))
		heartbeat_pb2_grpc.HearBeatStub(a[0][4]).isAlive(heartbeat_pb2.NodeInfo())
		return a[0][3]

if __name__ == '__main__':
	n= nodeSelect()
	print(n.leastUtilizedNode(None))
