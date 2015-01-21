import os
import sys

class Job:
	stat_id = 0
	def __init__(self, start, tasks, jobid):
		self.taskId = 0
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
		self.needNewAllocation = True
		self.readyTaskIndex = 0
		self.numTasksFinished = 0
		self.numTasksRunning = 0
		self.domShare = 0.0
		
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
		for task in tasks:
			task.setTaskId(self.taskId)
			self.taskId += 1

		self.tasksReady.extend(tasks)
		#print "ADD TASKS ==== JOBID : ", self.jobid , " NUMBER OF READY TASKS : ", len(self.tasksReady)
	def getDomShare(self):
		#print self
		return self.domShare

	def getCpuUsage(self):
		return self.cpuUsage

	def getMemUsage(self):
		return self.memUsage

	def getTasksRunning(self):
		return self.numTasksRunning

	def hasTasksReady(self):
		if self.readyTaskIndex == len(self.tasksReady):
			return False
		else:
			return True 

	def hasTasksRunning(self):
		if self.numTasksRunning == 0:
			return False
		else:
			return True

	def getReadyTask(self):
		return self.tasksReady[self.readyTaskIndex]

	def getReadyTasks(self):
		#print "GET TASKS READY === JOBID : ", self.jobid, "NUMBER OF READY TASKS : " , len(self.tasksReady)
		return self.tasksReady[self.readyTaskIndex:]

	def markAsRunning(self, task, time):
		if self.haveStarted == False:
			self.haveStarted = True
			self.actualStartTime =  time

		self.readyTaskIndex += 1
		#print "Allocated task , ", self.readyTaskIndex, "in job : ", self.jobid
		#self.tasksReady.remove(task)
		self.tasksRunning.append(task)
		self.numTasksRunning += 1
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )	
		#print "Job ID : ", self.jobid, "Num Tasks ready:", len(self.tasksReady), "Num tasks running:", len(self.tasksRunning)

	def taskDone(self, task, time):
		#self.tasksRunning.remove(task)
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem
		self.numTasksRunning -= 1
		self.numTasksFinished += 1

		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )

		#print "Job ID : ", self.jobid, "Num Tasks ready:", len(self.tasksReady), "Num tasks running:", len(self.tasksRunning)


	def done(self):
		return

	def getActualStartTime(self):
		return self.actualStartTime

	def getNumTasks(self):
		return numTasks

	def allTasksAllocated(self):
		if self.readyTaskIndex == len(self.tasksReady):
			return True
		else:
			return False

	def getAllTasksAllocatedStatus(self):
		if len(self.tasksReady) > 0:
			print "Panic"
		retValue = self.needNewAllocation
		self.needNewAllocation = False
		return retValue

	def __str__(self):
		return "Job : %d, Num Tasks : %d, Start Time : %d, Queue : %s , Util : (%.2f, %.2f) , NumTasksLeft : %d " % ( self.jobid, self.numTasks, self.start, self.queue.name, self.memUsage, self.cpuUsage, len(self.tasksReady) - self.readyTaskIndex )

	def __cmp__(self, other):

		if not isinstance(other, Job):
			return 1

		if self.getDomShare() <= other.getDomShare():
			return -1
		else:
			return 1
