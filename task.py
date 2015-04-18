import os
import sys

class Task:
	statid = 0
	def __init__(self, dur):
		self.duration = dur
		#self.taskid = statid
		self.taskid = 0
		self.machine = None

	def __init__(self, mem , cpu , dur):
		self.duration = dur
		self.cpu = cpu
		self.mem = mem
		self.machine = None

	def setJob(self, job):
		self.job = job

	def setTaskId(self, taskid):
		self.taskid = taskid

	def addMachine(self, m):
		self.machine = m

	def __str__(self):
		if self.machine != None:
			return "jobid:%d, taskid:%d, duration:%d machinid:%d (mem,cpu):(%.2f,%.2f)" %(self.job.jobid, self.taskid, self.duration, self.machine.machineId, self.mem, self.cpu)
		else:
			return "jobid:%d, taskid:%d, duration:%d machinid:%s (mem,cpu):(%.2f,%.2f)" %(self.job.jobid, self.taskid, self.duration, "None" , self.mem, self.cpu)

  
