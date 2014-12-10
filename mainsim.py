import os
import sys
from cluster import *
from Queue import *
from DRFQueue import *
from eventgenerator import *
from event import *
from elephantQueue import *
from CDRF import *
from sorter import *
import heapq
from statlogger import *

INF = sys.maxint

class Simulator:
	def __init__(self, cluster, elephantNumTasks):
		self.cluster = cluster
		self.evQueue = []
		self.numStartedJobs = 0
		self.maxJobs = 10000
		self.needNewElephantAllocation = False
		self.allocation = {}
		self.numJobsCompleted = 0
		self.numElephantsFinished = 0
		self.numMiceFinished = 0
		self.elephantTaskThreshold = elephantNumTasks
		self.numMiceStarted = 0
		self.numElephantStarted = 0

		self.statLoggers = []

	def setStopTime(self, stopTime):
		self.stopTime = stopTime

	def setMaxJobs(self, maxJobs):
		self.maxJobs = maxJobs


	def getCurrentElephants(self):
		if len(self.elephantQueue.jobs) == 0:
			return None
		#print "NUMBER OF PLAYERS : ", len(self.elephantQueue.jobs)
		players = numpy.zeros((len(self.elephantQueue.jobs) , 2))	
		for i in range(len(self.elephantQueue.jobs)):
			#print "PLAYER : ", self.elephantQueue.jobs[i] 
			job = self.elephantQueue.jobs[i]
			players[i,0] = job.mem
			players[i,1] = job.cpu

		return players
	
	def elephantAllocation(self):
		if self.needNewElephantAllocation == False:
			if len(self.elephantQueue.jobs) == 0:
				return None
			else:
				return self.allocation
		players = self.getCurrentElephants()
		if players == None:
			return None

		alloc = computeCDRF(numpy.array(self.cluster.machineConfig), numpy.array(players) , [ numpy.array(self.cluster.machinesPerType) ], 1, 0)
		self.allocation = {}
		for i in range(len(self.elephantQueue.jobs)):
			jobid = self.elephantQueue.jobs[i].jobid
			self.allocation[jobid] = {}
			for j in range(len(self.cluster.machineConfig)):
				self.allocation[jobid][j] = alloc[ i* len(self.cluster.machines) + j*len(self.cluster.machinesByType[j]) ]

		return self.allocation
		
	def handleEvents(self):

		totNumEvents = 0
		self.needNewElephantAllocation = False
		if (len(self.evQueue) == 0):
			return None
		time =  self.evQueue[0][0]
		#print "Handling events for time : ", time	
		print >> sys.stderr, "Handling events for time : ", time
		print >> sys.stderr, "Num jobs started : " , self.numElephantStarted + self.numMiceStarted , " (%d, %d) " % (self.numElephantStarted, self.numMiceStarted )
		print >> sys.stderr, "Num jobs completed : " , self.numJobsCompleted , " (%d, %d) " % (self.numElephantsFinished, self.numMiceFinished )
		print >> sys.stderr, "Num jobs running : ( %d, %d ) " % ( len(self.elephantQueue.jobs) , len(self.miceQueue.jobs))
		print >> sys.stderr, "Fully running task jobs : ( %d, %d ) " % ( len(self.elephantQueue.fullyRunningJobs), len(self.miceQueue.fullyRunningJobs) ) 
		print >> sys.stderr, "Length of waitlist : (%d , %d) " % ( len(self.elephantQueue.waitList) , len(self.miceQueue.waitList) )
		#print >> sys.stderr, "\n"
		#print >>sys.stderr, "Times : "
		#for i in range(len(self.evQueue)):
		#	print >> sys.stderr, self.evQueue[i][0] ,
		#print >>sys.stderr


		#print "Peek time : ", self.queuePeekTime()
		while not len(self.evQueue) == 0 and self.evQueue[0][0] == time :
			totNumEvents += 1
			ev = heapq.heappop(self.evQueue)[1]
			#print "Handling Event : ", ev
			if ev.eventType == "JOB":
				#print "New Job"
				startedJob = ev.data
				#print "Start job : ", startedJob
				startedJob.setActualStart(time)
				queue = startedJob.queue
				queue.addJob(startedJob)

				if startedJob.isElephant():
					#print "#1 : self.needNewElephantAllocation = True"
					self.needNewElephantAllocation = True
					self.numElephantStarted += 1
				else:
					self.numMiceStarted += 1

				if self.numStartedJobs < self.maxJobs :
					self.numStartedJobs += 1
					q = startedJob.getQueue()
					j = q.readNextJob()
					if j!= None:
						heapq.heappush(self.evQueue, (j.start, Event(j.start, "JOB", j)))
						#print >> sys.stderr, "Pushed ", j.start

			elif ev.eventType == "TASKDONE":
				t = ev.data
				j = t.job
				j.taskDone(t, time)
				q = j.getQueue()
				q.taskEnded(t, time)
				if not j.hasTasksReady() and not j.hasTasksRunning():
					heapq.heappush(self.evQueue,  (time, Event(time, "JOBDONE", j)))
					#print >> sys.stderr, "pushed ", time

				elif not j.hasTasksReady() :
					if j.isElephant() :
						#print "#2: self.needNewElephantAllocation = True"
						self.needNewElephantAllocation = True
						if q.hasWaitlistedJobs():
							job = q.getWaitlistedJob()
							if job == None:
								print "PANIC"
								exit()
							if job.start < time:
								job.start = time
							heapq.heappush(self.evQueue, (job.start , Event(job.start , "JOB", job)))
								
					

			elif ev.eventType == "JOBDONE":
				j = ev.data
				j.done()
				for s in self.statLoggers:
					s.jobDone(time, j)

				q = j.getQueue()
				if q.hasWaitlistedJobs():
					job = q.getWaitlistedJob()
					if job == None:
						print "PANIC"
						exit()
					if job.start < time:
						job.start = time
					heapq.heappush(self.evQueue, (job.start , Event(job.start , "JOB", job)))
					#print >> sys.stderr, "pushed ", time
				q.jobCompleted(j)

				self.numJobsCompleted += 1
				if j.isElephant() == True:
					self.numElephantsFinished += 1
					#print "#3: self.needNewElephantAllocation = True"
					self.needNewElephantAllocation = True
				else:
					self.numMiceFinished += 1


		print >> sys.stderr, "Number of events handled : ", totNumEvents
		print >> sys.stderr, "\n\n"
		return time

	def allocateDRF(self, time):
		allocated = True
	   	while allocated == True:
			allocated = False
			#print "Getting job"
			if len(self.miceQueue.jobs) == 0:
				break
			job = self.miceQueue.getJob()
			#print "Got job : ", job
			for t in job.getReadyTasks():
				#print "Task : " , t
				for m in self.cluster.machines:
					#print "machine : ", m
					if m.canAddTask(t):
						#print "Machine can add task"
						allocated = True
						m.addTask(t)
						job.markAsRunning(t, time)  
						self.miceQueue.taskStarted(t)
						heapq.heappush( self.evQueue, (time + t.duration , Event(time + t.duration, "TASKDONE", t)))
						#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
						break
					#print "Machine cannot add task", t
				if allocated == True:
					break


		#print "Retuning from DRF allocation function"

	def allocateElephants(self, time):
		#print "Needs new elephant allocation : ", self.needNewElephantAllocation 
		alloc = self.elephantAllocation()
		if alloc == None:
			#print "No elephants"
			return 
		#print "Elephant allocation : ", alloc
		for elephant in alloc:
			job = self.elephantQueue.getJobById(elephant)
			#print "Job : ", job
			if job in self.elephantQueue.fullyRunningJobs :
				#print "==== ELEPHANT JOB FULLY RUNNING"
				continue

			for machineType in alloc[elephant]:
				numTasks = alloc[elephant][machineType]
				for m in self.cluster.machinesByType[machineType]:
					numTasksAlreadyRunning = m.getNumTasksJob(elephant)
					#print "numTasksAlready Running : ", numTasksAlreadyRunning, numTasks
					if numTasksAlreadyRunning >= numTasks:
						continue
					for i in range(numTasksAlreadyRunning, int(numTasks)):
						if not job.hasTasksReady():
							break
						#print job , m
						t = job.getReadyTasks()[0]
						if m.canAddTask(t):
							m.addTask(t)
							job.markAsRunning(t, time)
							self.elephantQueue.taskStarted(t)
							heapq.heappush(self.evQueue, (time + t.duration, Event(time + t.duration, "TASKDONE", t))) 
							#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
						else:
							break


	def initializeQueues(self):
		mice_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold, False )
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, 50 , mice_gen )


		elephant_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold , True)
		self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, 0 , elephant_gen , True )
	
	def start(self):
		#print "Going to read mice job"
		j = self.miceQueue.readNextJob()
		for s in self.statLoggers:
			s.start()

		#print "Read initial mice job"
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 
		#j = self.elephantQueue.readNextJob()
		#heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j)))
		#print "Read initial elephant job"

		time = None
		while not len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs :
			time = self.handleEvents()
			if time == None:
				break
			#print "Allocating elephants "
			#self.allocateElephants(time)
			#print "Allocatin mice"	
			self.allocateDRF(time)
			#print "Finished this round"

		print "Simulation complete"
		self.finish(time)

	def DRFOnly(self):
		for s in self.statLoggers:
			s.start()


		#setting job threshold for elephants to INFINITY
		mice_gen = EventGenerator("sorted_jobs-exp-small", sys.maxint, False )   
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, 50 , mice_gen )


		elephant_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold , True)
		self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, 0 , elephant_gen , True )
	

		j = self.miceQueue.readNextJob()
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 

		time = None
		while not len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs :
			time = self.handleEvents()
			if time == None:
				break
			self.allocateDRF(time)

		print "Simulation complete"
		self.finish(time)


	def DDRF(self):

		for s in self.statLoggers:
			s.start()


		mice_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold, False )
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, 50 , mice_gen )


		elephant_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold , True)
		self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, 50 , elephant_gen , True )
	
		j = self.miceQueue.readNextJob()
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 
		j = self.elephantQueue.readNextJob()
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j)))

		time = None
		while not len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs :
			time = self.handleEvents()
			if time == None:
				break
			self.allocateElephants(time)
			self.allocateDRF(time)

		print "Simulation complete"
		self.finish(time)


	def finish(self, time):
		for sl in self.statLoggers:
			sl.finish(time)

	def addStatLogger(self, sl):
		self.statLoggers.append(sl)

def DRFOnly():
	global INF
	elephantNumTasks = INF

	
    	general = [ 30.0 , 30.0 ]
    	mem_optimized = [ 30.0 , 4.0 ]
    	cpu_optimized = [ 16 , 30.0 ]


	machineConfig = [ general, mem_optimized, cpu_optimized ]
  	machinesPerType = [ 500 , 500 , 500  ]
	cluster = Cluster(machineConfig, machinesPerType, 3.0 , 5.0 , INF , 1.0 , 0.0 )

	sl = JobFinishLogger("delaysDRF.csv")	
	sim = Simulator(cluster , elephantNumTasks)
	sim.addStatLogger(sl)
	sim.initializeQueues()
	sim.DRFOnly()

def DDRF():
	global INF
	elephantNumTasks = 1000

	
    	general = [ 30.0 , 30.0 ]
    	mem_optimized = [ 30.0 , 4.0 ]
    	cpu_optimized = [ 16 , 30.0 ]


	machineConfig = [ general, mem_optimized, cpu_optimized ]
  	machinesPerType = [ 500 , 500 , 500  ]
	cluster = Cluster(machineConfig, machinesPerType, 3.0 , 5.0 , elephantNumTasks , 0.8 , 0.8 )

	sl = JobFinishLogger("delaysDDRF.csv")	
	sim = Simulator(cluster , elephantNumTasks)
	sim.addStatLogger(sl)
	sim.initializeQueues()
	sim.DDRF()




if __name__== "__main__":
	DDRF()
	#DRFOnly()

