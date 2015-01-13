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
		return "jobid:%d, taskid:%d, machine:%s " %(self.job.jobid, self.taskid, str(self.machine))

  
