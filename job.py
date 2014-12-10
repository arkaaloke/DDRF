import os
import sys

class Job:
	stat_id = 0
	def __init__(self, start, tasks, jobid):
		self.start = start
		#self.jobid = Job.stat_id
		self.jobid = jobid
		Job.stat_id += 1
		self.memUsage = 0
		self.cpuUsage = 0
		self.tasksReady = []
		self.tasksRunning = []
		self.haveStarted = False
		self.actualStartTime = None
		for t in tasks:
			t.setJob(self)
			self.mem = t.mem
			self.cpu = t.cpu

		self.numTasks = len(tasks)
		self.addTasks(tasks)
		self.queue = None

	def isElephant(self):
		if self.queue.getElephantStatus() == True:
			return True
		else:	
			return False

	def setActualStart(self, time):
		self.actualStartTime = time

	def setQueue(self, queue):
		self.queue = queue

	def getQueue(self):
		return self.queue

	def setClusterParams(self, totCpu, totMem):
		self.totCpu = totCpu
		self.totMem = totMem

	def addTasks(self, tasks):
		self.tasksReady.extend(tasks)
		#print "ADD TASKS ==== JOBID : ", self.jobid , " NUMBER OF READY TASKS : ", len(self.tasksReady)
	def getDomShare(self):
		#print self
		return max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )

	def getCpuUsage(self):
		return self.cpuUsage

	def getMemUsage(self):
		return self.memUsage

	def getTasksRunning(self):
		return len(self.tasksRunning)

	def hasTasksReady(self):
		if len(self.tasksReady) == 0:
			return False
		else:
			return True 

	def hasTasksRunning(self):
		if len(self.tasksRunning) == 0:
			return False
		else:
			return True

	def getReadyTasks(self):
		#print "GET TASKS READY === JOBID : ", self.jobid, "NUMBER OF READY TASKS : " , len(self.tasksReady)
		return self.tasksReady

	def markAsRunning(self, task, time):
		if self.haveStarted == False:
			self.haveStarted = True
			self.actualStartTime =  time

		self.tasksReady.remove(task)
		self.tasksRunning.append(task)
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

	def taskDone(self, task, time):
		self.tasksRunning.remove(task)
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem


	def done(self):
		return

	def getActualStartTime(self):
		return self.actualStartTime

	def getNumTasks(self):
		return numTasks

	def __str__(self):
		return "Job : %d, Num Tasks : %d, Start Time : %d, Queue : %s , Util : (%.2f, %.2f) , NumTasksLeft : %d " % ( self.jobid, self.numTasks, self.start, self.queue.name, self.memUsage, self.cpuUsage, len(self.tasksReady) )

