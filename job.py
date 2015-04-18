import os
import sys
import random

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
		self.taskDoneEvent = {}
		for t in tasks:
			t.setJob(self)
			self.mem = t.mem
			self.cpu = t.cpu

		self.taskQueueTime = {}
		self.taskStartTime = {}
		self.numTasks = len(tasks)
		self.addTasks(tasks)
		self.queue = None
		self.needNewAllocation = True
		self.readyTaskIndex = 0
		self.numTasksFinished = 0
		self.numTasksRunning = 0
		self.domShare = 0.0
		self.typeNumMachines = 0
		self.bestMachineType = None

	def addTaskDoneEvent(self, task, event):
		#print "ADDED TASKDONEVENT" , task
		self.taskDoneEvent[task.taskid] = event

	def getTaskDoneEvent(self, task):
		#print self, self.taskDoneEvent 
		return self.taskDoneEvent[task.taskid]

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


	def setClusterParams(self, totCpu, totMem, machinesByType):
		self.machineTypeLowerBound = None
		self.machineTypeUpperBound = None
		self.totCpu = totCpu
		self.totMem = totMem
		self.standalone = 0
		s = -1 * sys.maxint
		numMachinesTillNow = 0
		bestMachineType = 0
		for t in range(len(machinesByType)):
			numMachines = len(machinesByType[t])
			machine_mem = machinesByType[t][0].mem
			machine_cpu = machinesByType[t][0].cpu
			numTasks = min ( int(machine_mem / self.mem) , int(machine_cpu / self.cpu) )
			self.standalone += numTasks * numMachines
			#score = -1 * (self.tasksReady[0].mem / machine_mem + self.tasksReady[0].cpu / machine_cpu )
			score = min( machine_mem / self.tasksReady[0].mem , machine_cpu / self.tasksReady[0].cpu )
			#print "BEST FIT SCORE : ", score
			if self.machineTypeLowerBound == None:
				self.machineTypeLowerBound = 0
				self.machineTypeUpperBound = len(machinesByType[t]) - 1
				bestMachineType = t
				s = score
			elif score > s:
				self.machineTypeLowerBound = numMachinesTillNow
				self.machineTypeUpperBound = numMachinesTillNow + len(machinesByType[t]) - 1
				s = score
				bestMachineType = t

		  
			numMachinesTillNow += len(machinesByType[t])

		self.bestMachineType = bestMachineType
		#print "BEST FIT : ", self.tasksReady[0] , "Machine Type : " , bestMachineType , "Bounds : " , self.machineTypeLowerBound , self.machineTypeUpperBound , "Score : ", s 
		self.typeNumMachines = machinesByType[bestMachineType]
		#print "job mem : ", self.mem, " job cpu : ", self.cpu, " job standalone  : ", self.standalone

	def getBestMachineType(self):
		return self.bestMachineType

	def getTypeNumMachines(self):
		return self.typeNumMachines

	def getRandomMachine(self):
		return random.randint(self.machineTypeLowerBound, self.machineTypeUpperBound) 

	def addTasks(self, tasks):
		for task in tasks:
			self.taskQueueTime[self.taskId] = None
			self.taskStartTime[self.taskId] = 0
			self.taskDoneEvent[self.taskId] = None
			task.setTaskId(self.taskId)
			self.taskId += 1
		self.tasksReady.extend(tasks)
		#print "ADD TASKS ==== JOBID : ", self.jobid , " NUMBER OF READY TASKS : ", len(self.tasksReady)
	def getDomShare(self):
		#print self
		return float(self.numTasksRunning) / float(self.standalone)
		#return self.domShare

	def getCpuUsage(self):
		return self.cpuUsage

	def getMemUsage(self):
		return self.memUsage

	def getTasksRunning(self):
		return self.numTasksRunning

	def hasTasksReady(self):
		#if self.readyTaskIndex == len(self.tasksReady):
		if len(self.tasksReady) == 0 :
			return False
		else:
			return True 

	def hasTasksRunning(self):
		if self.numTasksRunning == 0:
			return False
		else:
			return True

	def getReadyTask(self):
		#return self.tasksReady[self.readyTaskIndex]
		return self.tasksReady[0]
	def getReadyTasks(self):
		#print "GET TASKS READY === JOBID : ", self.jobid, "NUMBER OF READY TASKS : " , len(self.tasksReady)
		#return self.tasksReady[self.readyTaskIndex:]
		return self.tasksReady
	def queueOnMachine(self, task, time):
		if self.haveStarted == False:
			self.haveStarted = True
			self.actualStartTime =  time

		self.taskQueueTime[task.taskid] = time
		#self.taskQueueTime[self.readyTaskIndex] = time
		#self.readyTaskIndex += 1
		self.tasksRunning.append(task)

		self.tasksReady.remove(task)
		self.numTasksRunning += 1
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )	
		#print "Job ID : ", self.jobid, "Num Tasks ready:", len(self.tasksReady), "Num tasks running:", len(self.tasksRunning)


	def dequeueOnMachine(self, task, time):
		self.taskStartTime[task.taskid] = time
		print "TASK DEQUEUED : Time = ", time, "Added Task = ", task , "; Job = " , task.job , "; Machine	=", task.machine, "QUEUE WAIT = ", self.taskStartTime[task.taskid] - self.taskQueueTime[task.taskid]


	def markAsRunning(self, task, time):
		if self.haveStarted == False:
			self.haveStarted = True
			self.actualStartTime =  time

		#self.taskStartTime[self.readyTaskIndex] = time
		self.taskStartTime[task.taskid] = time
		#self.readyTaskIndex += 1
		#print "Allocated task , ", self.readyTaskIndex, "in job : ", self.jobid
		print "Task : ", task, "tasksReady : " , self.tasksReady[0]
		self.tasksReady.remove(task)
		self.tasksRunning.append(task)
		self.numTasksRunning += 1
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )	
		#print "Job ID : ", self.jobid, "Num Tasks ready:", len(self.tasksReady), "Num tasks running:", len(self.tasksRunning)

	def taskPreempted(self, task, time):
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem
		self.numTasksRunning -= 1
		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )
		self.tasksReady.append(task)
		self.tasksRunning.remove(task)	

		if self not in self.queue.jobs:
			self.queue.jobs.append(self)
			self.queue.fullyRunningJobs.remove(self)

	def taskDone(self, task, time):
		#self.tasksRunning.remove(task)
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem
		self.numTasksRunning -= 1
		self.numTasksFinished += 1

		self.domShare = max( float(self.cpuUsage) / float(self.totCpu) , float(self.memUsage) / float(self.totMem) )

		#print "Job ID : ", self.jobid, "Num Tasks ready:", len(self.tasksReady), "Num tasks running:", len(self.tasksRunning)


	def getNumTasksRemaining(self):
		return len(self.tasksReady)

	def done(self):
		return

	def getActualStartTime(self):
		return self.actualStartTime

	def getNumTasks(self):
		return numTasks

	def allTasksAllocated(self):
		#if self.readyTaskIndex == len(self.tasksReady):
		if len(self.tasksReady) == 0:
			return True
		else:
			return False

	def getAllTasksAllocatedStatus(self):
		#if len(self.tasksReady) != self.readyTaskIndex :
		if len(self.tasksReady) != 0:
			print "Panic"
		retValue = self.needNewAllocation
		self.needNewAllocation = False
		return retValue

	def __str__(self):
		#return "Job : %d, Num Tasks : %d, Start Time : %d, Queue : %s , Util : (%.2f, %.2f) , NumTasksLeft : %d " % ( self.jobid, self.numTasks, self.start, self.queue.name, self.memUsage, self.cpuUsage, len(self.tasksReady) - self.readyTaskIndex )
		return "Job : %d, Num Tasks : %d, Start Time : %d, Queue : %s , Util : (%.2f, %.2f) , NumTasksLeft : %d " % ( self.jobid, self.numTasks, self.start, self.queue.name, self.memUsage, self.cpuUsage, len(self.tasksReady) )


