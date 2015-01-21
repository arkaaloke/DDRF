import os
import sys
from clusterseparate import *
from Queue import *
from DRFQueue import *
from eventgenerator import *
from event import *
from elephantQueue import *
from CDRF import *
from sorter import *
import heapq
from statlogger import *
import time as tm
import signal

INF = sys.maxint


def signal_handler(signal, frame):
		print('You pressed Ctrl+C!')
		sys.exit(0)

def tracefunc(frame, event, arg, indent=[0]):
	  if event == "call":
		  indent[0] += 2
		  print tm.time(), "-" * indent[0] + "> call function", frame.f_code.co_name
	  elif event == "return":
		  print tm.time(), "<" + "-" * indent[0], "exit function", frame.f_code.co_name
		  indent[0] -= 2
	  return tracefunc

class Simulator:
	def __init__(self, cluster, elephantNumTasks):
		self.cluster = cluster
		self.evQueue = []
		self.numStartedJobs = 0
		self.maxJobs = sys.maxint
		self.needNewElephantAllocation = False
		self.allocation = {}
		self.numJobsCompleted = 0
		self.numElephantsFinished = 0
		self.numMiceFinished = 0
		self.elephantTaskThreshold = elephantNumTasks
		self.numMiceStarted = 0
		self.numElephantStarted = 0
		self.numLPInvocations = 0
		self.statLoggers = []
		self.lastIterationTime = tm.time()
		self.stopTime = sys.maxint

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

		print "Invoking an elephant allocation"
		alloc = computeCDRF(numpy.array(self.cluster.elephantMachineConfig), numpy.array(players) , [ numpy.array(self.cluster.elephantMachinesPerType) ], 1, 0)
		self.numLPInvocations += 1
		self.allocation = {}
		for i in range(len(self.elephantQueue.jobs)):
			jobid = self.elephantQueue.jobs[i].jobid
			self.allocation[jobid] = {}
			for j in range(len(self.cluster.elephantMachineConfig)):
				self.allocation[jobid][j] = alloc[ i* len(self.cluster.elephantMachines) + j*len(self.cluster.elephantMachinesByType[j]) ]

		return self.allocation
		
	def handleEvents(self):

		totNumEvents = 0
		self.needNewElephantAllocation = False
		if (len(self.evQueue) == 0):
			return None
		time =  self.evQueue[0][0]
		#print "Handling events for time : ", time	

		print "\n\n"
		clockTime = tm.time()
		timeTaken = clockTime - self.lastIterationTime
		print tm.time() , "STARTING HANDLING FOR TIME : ", time
  
		print >> sys.stderr, timeTaken

		self.lastIterationTime = clockTime
		print >> sys.stderr, "Handling events for time : ", time
		#print >> sys.stderr, "Num jobs started : " , self.numElephantStarted + self.numMiceStarted , " (%d, %d) " % (self.numElephantStarted, self.numMiceStarted )
		print >> sys.stderr, "Num jobs completed : " , self.numJobsCompleted , " (%d, %d) " % (self.numElephantsFinished, self.numMiceFinished )
		#print >> sys.stderr, "Num jobs running : ( %d, %d ) " % ( len(self.elephantQueue.jobs) , len(self.miceQueue.jobs))
		#print >> sys.stderr, "Fully running task jobs : ( %d, %d ) " % ( len(self.elephantQueue.fullyRunningJobs), len(self.miceQueue.fullyRunningJobs) ) 
		#print >> sys.stderr, "Length of waitlist : (%d , %d) " % ( len(self.elephantQueue.waitList) , len(self.miceQueue.waitList) )
		#print >> sys.stderr, "Utilization : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
		#		(  self.elephantQueue.memUsage , \
		#		   self.elephantQueue.cpuUsage , \
		#		   self.miceQueue.memUsage  , \
		#		   self.miceQueue.cpuUsage )
		#print >> sys.stderr, "Total : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
		#		(  float(self.cluster.totMem) , \
		#		   float(self.cluster.totCpu) , \
		#		   float(self.cluster.totMem) , \
		#		   float(self.cluster.totCpu) )
 

 

		#print >> sys.stderr, " Percent Utilization : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
		#		(  self.elephantQueue.memUsage / float(self.cluster.totMem) , \
		#		   self.elephantQueue.cpuUsage / float(self.cluster.totCpu) , \
		#		   self.miceQueue.memUsage / float(self.cluster.totMem) , \
		#		   self.miceQueue.cpuUsage / float(self.cluster.totCpu) )

		#print >> sys.stderr, " Num Tasks : elephant : (%d) , mice : (%d) " % (self.elephantQueue.numRunningTasks, self.miceQueue.numRunningTasks)
		print >> sys.stderr, " Num LP invocations : %s " % (self.numLPInvocations)
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
				#print "JOB : ", startedJob, startedJob.totCpu, startedJob.totMem 
				if startedJob.isElephant():
					#print "#1 : self.needNewElephantAllocation = True"
					print "NEED new elephant allocation because a new elephant has come"
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
				#print "Printing all tasks for job : ", j
				#for tk in j.getReadyTasks():
				#	print tk

				#print j , t, time, t.machine
				j.taskDone(t, time)
				machine = t.machine
				machine.deleteTask(t)
				q = j.getQueue()
				q.taskEnded(t, time)
				for s in self.statLoggers:
					s.taskDone(time, t)


				#if j.isElephant():
				#	print "Task finished in job : ", j
				#	if not j.hasTasksReady() and not j.hasTasksRunning():
				#		print "Going to add JOBDONE"
				#	elif not j.hasTasksReady():
				#		print "scheduling again"

				if not j.hasTasksReady() and not j.hasTasksRunning():
					heapq.heappush(self.evQueue,  (time, Event(time, "JOBDONE", j)))
					#print >> sys.stderr, "pushed ", time

				elif not j.hasTasksReady() :
					if j.isElephant() and j.getAllTasksAllocatedStatus() == True :
						#print "#2: self.needNewElephantAllocation = True"
						print "new elephant allocation because one elephant has exhausted all its tasks"
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
					print "Need new allocation because an elephant is done"
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

			freeMachines = self.cluster.getFreeMiceMachineArray() 
			job = self.miceQueue.getJob()
			#print "Got job : ", job
			t = job.getReadyTask()
			#print "Task : " , t
			if len(freeMachines[0]) == 0:
				break

			
			m = self.cluster.getMiceMachine(freeMachines[0][0])

			#for i in range(len(freeMachines[0])):
			#m = self.cluster.getMiceMachine(freeMachines[0][i])
			#print "machine : ", m
			#if m.canAddTask(t):
			#	print "Machine can add task"
			allocated = True
			m.addTask(t)
			job.markAsRunning(t, time)  
			self.miceQueue.taskStarted(t)
			heapq.heappush( self.evQueue, (time + t.duration , Event(time + t.duration, "TASKDONE", t)))
			if t.machine == None:	
				print "Machine is NONE : ", t.machine
				#print "Printing all tasks for job : ", job
				#for tk in job.getReadyTasks():
				#	print tk


				#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
				break
				#print "Machine cannot add task", t

		#print "Retuning from DRF allocation function"

	def allocateElephants(self, time):
		#print "Needs new elephant allocation : ", self.needNewElephantAllocation 
		alloc = self.elephantAllocation()
		if alloc == None:
			#print "No elephants"
			return 
		#print "Elephant allocation : ", alloc
		freeMachines = self.cluster.getFreeElephantMachineArray()	
		for i in range(len(freeMachines[0])):
			m = self.cluster.getElephantMachine(freeMachines[0][i])
			
			for elephant in alloc:
				if not m.isMachineFree():
					break
				job = self.elephantQueue.getJobById(elephant)
				#print "Job : ", job
				if job in self.elephantQueue.fullyRunningJobs :
					#print "==== ELEPHANT JOB FULLY RUNNING"
					continue

				machineType = self.cluster.getMachineType(m)

				numTasks = alloc[elephant][machineType]
				numTasksAlreadyRunning = m.getNumTasksJob(elephant)
				#print "numTasksAlready Running : ", numTasksAlreadyRunning, numTasks
				if numTasksAlreadyRunning >= numTasks:
					#print "Number of tasks already running >= numTasks"
					continue
				for i in range(numTasksAlreadyRunning, int(numTasks)):
					if not job.hasTasksReady():
						#print "Job", job, "has no tasks ready"
						break
					#print job , m
					t = job.getReadyTask()
					if m.canAddTask(t):
						m.addTask(t)
						job.markAsRunning(t, time)
						self.elephantQueue.taskStarted(t)
						heapq.heappush(self.evQueue, (time + t.duration, Event(time + t.duration, "TASKDONE", t)))
						if t.machine == None:
							print "Machine of ", t, "is None"
						for s in self.statLoggers:
							s.newTask(time, t)


						#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
					else:
						#print "Machine", m, "cannot add task"
						break


	def initializeQueues(self):
		mice_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold, False )
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, sys.maxint , mice_gen )

		elephant_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold , True)
		self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, sys.maxint , elephant_gen , True )
	
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
		while not (len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs or time > self.stopTime ):
			print >> sys.stderr,  "=========STOP TIME : ", self.stopTime
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

	def DRFOnly(self, jobfile, miceQueueSize=sys.maxint ):
		for s in self.statLoggers:
			s.start()


		#setting job threshold for elephants to INFINITY
		mice_gen = EventGenerator( jobfile , sys.maxint, False )   
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, miceQueueSize , mice_gen )


		#elephant_gen = EventGenerator("sorted_jobs-exp-small", self.elephantTaskThreshold , True)
		#self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, 0 , elephant_gen , True )
	

		j = self.miceQueue.readNextJob()
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 

		time = None
		while not (len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs or time > self.stopTime):
			time = self.handleEvents()
			if time == None:
				break
			self.allocateDRF(time)

		print "Simulation complete"
		print "length of event queue : ", len(self.evQueue)
		self.finish(time)


	def DDRF(self, jobfile, miceQueueSize=sys.maxint, elephantQueueSize=sys.maxint):

		for s in self.statLoggers:
			s.start()


		#maxQueueSize = 10000000
		mice_gen = EventGenerator( jobfile , self.elephantTaskThreshold, False )
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, miceQueueSize , mice_gen )


		elephant_gen = EventGenerator( jobfile , self.elephantTaskThreshold , True)
		self.elephantQueue = ElephantQueue("elephant", None, 10000000, self.cluster, elephantQueueSize , elephant_gen , True )
	
		j = self.miceQueue.readNextJob()
		if j != None:
			heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 
		j = self.elephantQueue.readNextJob()
		if j != None:
			heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j)))

		time = None
		while not (len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs or time > self.stopTime ):
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

def DRFOnly(jobfile, stopTime=30000, logfile="delaysDRF.csv"):
	global INF
	elephantNumTasks = INF

	
	general = [ 30.0 , 30.0 ]
	mem_optimized = [ 30.0 , 4.0 ]
	cpu_optimized = [ 16 , 30.0 ]
	startTime = tm.time()

	machineConfig = [ general, mem_optimized, cpu_optimized ]
  	machinesPerType = [ 300 , 300 , 300  ]
	cluster = ClusterSeparate(machineConfig, machinesPerType, 3.0 , 3.0 , 1.0 )
	sl = JobFinishLogger(logfile)	
	sim = Simulator(cluster , elephantNumTasks)
	sim.addStatLogger(sl)
	sim.initializeQueues()
	sim.setStopTime(stopTime)
	sim.DRFOnly(jobfile )

	finish = tm.time()
	print startTime , finish , "Time taken : " , finish-startTime

def DDRF( jobfile, stopTime=30000, logfile="delaysDDRF.csv"):
	global INF
	elephantNumTasks = 10000

	
	general = [ 30.0 , 30.0 ]
	mem_optimized = [ 30.0 , 4.0 ]
	cpu_optimized = [ 16 , 30.0 ]
	startTime = tm.time()

	machineConfig = [ general, mem_optimized, cpu_optimized ]
  	machinesPerType = [ 300 , 300 , 300  ]
	cluster = ClusterSeparate(machineConfig, machinesPerType, 3.0 , 3.0 , 0.2 )

	sl = JobFinishLogger(logfile)	
	#el = ElephantLogger("elephant.csv")
	sim = Simulator(cluster , elephantNumTasks)
	sim.addStatLogger(sl)
	sim.setStopTime(stopTime)
	#sim.addStatLogger(el)
	sim.initializeQueues()
	sim.DDRF(jobfile, sys.maxint, 50)

	finish = tm.time()

	print startTime , finish , "Time taken : " , finish-startTime


if __name__== "__main__":
	#sys.settrace(tracefunc)
	signal.signal(signal.SIGINT, signal_handler)
	if sys.argv[1].strip() == "drf":
		DRFOnly( "sorted_jobs-exp", 86400)
	elif sys.argv[1].strip() == "ddrf":
		DDRF( "sorted_jobs-exp", 86400)
	#DRFOnly()

