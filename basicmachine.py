import os
import sys

class BasicMachine:
	ele_stat_id = 0
	mice_stat_id = 0
	def __init__(self, cpu, mem, minCpu, minMem, cluster, jobType):
		self.cpu = cpu
		self.mem = mem
		self.minCpu = minCpu
		self.minMem = minMem
		
		self.cpuUsage = 0
		self.memUsage = 0

		self.jobType = jobType

		self.cluster = cluster
		if jobType == "mice":
			self.machineId = BasicMachine.mice_stat_id
			BasicMachine.mice_stat_id += 1

		elif jobType == "elephant":
			self.machineId = BasicMachine.ele_stat_id
			BasicMachine.ele_stat_id += 1

		self.tasks = 0

		self.tasksByJob = {}
		
		self.isFree = True

	def __str__(self):
		return "%s - ID : %s , (%d, %d) " % (self.jobType, self.machineId, self.mem, self.cpu)

	def canAddTask(self, task):
		pass
	
		
	def addTask(self, task):
		pass

	def deleteTask(self, task):
		pass

	def getMachineId(self):
		return self.machineId

	def getNumTasksJob(self, jobid):
		if jobid not in self.tasksByJob :
			return 0
		else:
			return self.tasksByJob[jobid]


