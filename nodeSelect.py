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
		self.dummy= []
		activeNodeObj.channelRefresh()
		self.activeIpList = activeNodeObj.getActiveIpsDict()
		print(self.activeIpList)

	def leastUtilizedNode(self, serverAddress):
		min= float("-inf")
		for ip, channel in self.activeIpList.items():
			if ip == serverAddress:
				continue
			stub = heartbeat_pb2_grpc.HearBeatStub(channel)
			response = stub.isAlive(heartbeat_pb2.NodeInfo())

			self.dummy.append((response.disk_space, response.used_mem, response.cpu_usage, ip))
		if len(self.dummy)==0:
			return 9999
		print("dummy", self.dummy)
		a=sorted(self.dummy,key=lambda x: (x[0], x[1]))
		return a[0][3]

if __name__ == '__main__':
	n= nodeSelect()
	print(n.leastUtilizedNode())
