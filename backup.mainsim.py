import os
import sys
from onecluster import *
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
import random

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
		self.preemptedTasks = []
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
		self.utilStats = {}
		self.cluster.setUtilStats(self.utilStats)
		self.taskdoneElephantMachines = []
		self.numTasksFinishedInterval = 0
		self.simulationTime = None
		self.perJobAllocation = {}
		self.miceQueueMachines = []
		self.numTasksQueued = 0
		self.medianElephantShare = 0.0
		# HACK !!!!
		for i in range(0, 300):
			self.perJobAllocation[i] = 0

	def setStopTime(self, stopTime):
		self.stopTime = stopTime

	def setMaxJobs(self, maxJobs):
		self.maxJobs = maxJobs

	def getCurrentElephants(self):
		if len(self.elephantQueue.jobs) == 0:
			return [ None , None ]
		print "NUMBER OF PLAYERS : ", len(self.elephantQueue.jobs)
		players = numpy.zeros((len(self.elephantQueue.jobs) , 2))	
		requests = numpy.zeros( len(self.elephantQueue.jobs) )

		for i in range(len(self.elephantQueue.jobs)):
			#print "PLAYER : ", self.elephantQueue.jobs[i] 
			job = self.elephantQueue.jobs[i]
			players[i,0] = job.mem
			players[i,1] = job.cpu
			requests[i] = job.getNumTasksRemaining()
			#requests.append( job.getNumTasksRemaining() )
 
		#return ( players, requests ) 
		return [ players , requests ]

	def elephantAllocation(self, time):
		if self.needNewElephantAllocation == False:
			if len(self.elephantQueue.jobs) == 0:
				return None
			else:
				return self.allocation
		[ players , requests ]  = self.getCurrentElephants()
		print "Players : " , players , "Requests : ", requests
		if players == None:
			return None

		print "LP -> invokation : ", time
		print "LP -> tasks finished in the meantime : ", self.numTasksFinishedInterval , " , at time : ", time
		self.numTasksFinishedInterval = 0
		start = tm.time()

		#print "Calling CDRF with " , numpy.array(self.cluster.elephantMachineConfig), numpy.array(players) , [ numpy.array(self.cluster.elephantMachinesPerType) ] , requests 
		alloc = computeCDRF(1, numpy.array(self.cluster.elephantMachineConfig), numpy.array(players) , [ numpy.array(self.cluster.elephantMachinesPerType) ], 1, 0 )
		self.numLPInvocations += 1
		self.allocation = {}
		for i in range(len(self.cluster.machines)):
				for j in range(len(self.elephantQueue.jobs)):
					jobid = self.elephantQueue.jobs[j].jobid
					if jobid not in self.allocation:
						self.allocation[jobid] = {}
					self.allocation[jobid][ self.cluster.getMachine(i) ] =  alloc[ i * len(self.elephantQueue.jobs) + j ] 

		for job in self.allocation:
			self.perJobAllocation[job] = 0
			for m in self.allocation[job]:
				self.perJobAllocation[job] += self.allocation[job][m]


		#print "FAIRNESS", time, str(alloc)
		#print "FAIRNESS", time 
		#for job in sorted(self.allocation):
		#	for machine in sorted(self.allocation[job]):
		#		print "FAIRNESS", time, job, self.cluster.getMachineType(machine), self.cluster.machines.index(machine), self.allocation[job][machine] 

		for i in range(len(numpy.array(players))):
			print "p" + str(i) + " : " + str(players[i]) , 

		print
		end = tm.time()
		print "Simulation Time : %s, CDRF LP allocation %d took %.2f sec, num players : %d " % (str(self.simulationTime), self.numLPInvocations, end - start, len(self.elephantQueue.jobs) )

		return self.allocation
		
	def handleEvents(self):

		totNumEvents = 0
		if (len(self.evQueue) == 0):
			return None
		time =  self.evQueue[0][0]
		#print "Handling events for time : ", time	

		print "\n\n"
		clockTime = tm.time()
		timeTaken = clockTime - self.lastIterationTime
		print tm.time() , "STARTING HANDLING FOR TIME : ", time
  
		#print >> sys.stderr, timeTaken
		print timeTaken

		self.lastIterationTime = clockTime
		print >> sys.stderr, "Handling events for time : ", time
		print "Handling events for time : ", time
		print >> sys.stderr, "Num jobs started : " , self.numElephantStarted + self.numMiceStarted , " (%d, %d) " % (self.numElephantStarted, self.numMiceStarted )
		print >> sys.stderr, "Num jobs completed : " , self.numJobsCompleted , " (%d, %d) " % (self.numElephantsFinished, self.numMiceFinished )
		print self.numJobsCompleted , " (%d, %d) " % (self.numElephantsFinished, self.numMiceFinished )
		print >> sys.stderr, "Num tasks queue : %d " % (self.numTasksQueued)
		print time, ", Num tasks queue : %d " % (self.numTasksQueued)
		#print >> sys.stderr, "Num jobs running : ( %d, %d ) " % ( len(self.elephantQueue.jobs) , len(self.miceQueue.jobs))
		#print >> sys.stderr, "Fully running task jobs : ( %d, %d ) " % ( len(self.elephantQueue.fullyRunningJobs), len(self.miceQueue.fullyRunningJobs) ) 
		#print >> sys.stderr, "Length of waitlist : (%d , %d) " % ( len(self.elephantQueue.waitList) , len(self.miceQueue.waitList) )
		#print >> sys.stderr, "Utilization : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
		#		(  self.elephantQueue.memUsage , \
		#		   self.elephantQueue.cpuUsage , \
		#		   self.miceQueue.memUsage  , \
		#		   self.miceQueue.cpuUsage )
		print >> sys.stderr, "Total : Utilization : (%.2f) " % max(self.cluster.cpuUsage / self.cluster.totCpu , self.cluster.memUsage / self.cluster.totMem ) 
		print  time,";Total : Utilization : (%.2f) " % max(self.cluster.cpuUsage / self.cluster.totCpu , self.cluster.memUsage / self.cluster.totMem )
 

		print >> sys.stderr, " Percent Utilization : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
				(  self.elephantQueue.memUsage / float(self.cluster.totMem) , \
				   self.elephantQueue.cpuUsage / float(self.cluster.totCpu) , \
				   self.miceQueue.memUsage / float(self.cluster.totMem) , \
				   self.miceQueue.cpuUsage / float(self.cluster.totCpu) )

		print " Percent Utilization : elephant : (%.2f, %.2f) , mice : (%.2f, %.2f) " % \
				(  self.elephantQueue.memUsage / float(self.cluster.totMem) , \
				   self.elephantQueue.cpuUsage / float(self.cluster.totCpu) , \
				   self.miceQueue.memUsage / float(self.cluster.totMem) , \
				   self.miceQueue.cpuUsage / float(self.cluster.totCpu) )


		#print >> sys.stderr, " Num Tasks : elephant : (%d) , mice : (%d) " % (self.elephantQueue.numRunningTasks, self.miceQueue.numRunningTasks)
		print >> sys.stderr, " Num LP invocations : %s " % (self.numLPInvocations)
		#print " Num LP invocations : %s " % (self.numLPInvocations)
		#print >> sys.stderr, "\n"
		#print >>sys.stderr, "Times : "
		#for i in range(len(self.evQueue)):
		#	print >> sys.stderr, self.evQueue[i][0] ,
		print >>sys.stderr
		self.taskdoneElephantMachines = []

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
					print "LP -> NEED new elephant allocation because a new elephant has come : " , time
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
				if (time, ev) in self.preemptedTasks:
					print "Ignoring preempted taskdone"
					self.preemptedTasks.remove((time, ev))
					continue

				t = ev.data
				j = t.job
				#print "Printing all tasks for job : ", j
				#for tk in j.getReadyTasks():
				#	print tk

				print "Finished task : ", j , t, time, t.machine
				j.taskDone(t, time)
				machine = t.machine
				if j.isElephant():
					self.taskdoneElephantMachines.append(machine)

				machine.deleteTask(t, time)
				q = j.getQueue()
				q.taskEnded(t, time)
				for s in self.statLoggers:
					s.taskDone(time, t)

				if j.isElephant():
					self.numTasksFinishedInterval += 1

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
				else:
					self.numMiceFinished += 1


		#print >> sys.stderr, "Number of events handled : ", totNumEvents
		print "Number of events handled : ", totNumEvents
		#print >> sys.stderr, "\n\n"
		print "\n\n"
		self.simulationTime = time

		return time

	def allocateMiceDDRF(self, time):
		start = tm.time()
		allocated = True
		queue = self.miceQueue

			
		numMachineTypes = self.cluster.numMachineTypes
		for i in range(numMachineTypes):
			while allocated == True:
				allocated = False
				if len(queue.jobs) == 0:
					print "allocateMiceDDRF no jobs in queue"
					break

				job = queue.getJob()
				if job == None:
					break
				if job.getDomShare() > self.medianElephantShare and self.medianElephantShare != 0.0 : 
					break
				t = job.getReadyTask()
				print "Task next to run : ", t , t.mem, t.cpu

				all_free_mice_machines = self.cluster.getFreeMiceMachineArray()[0]
				if len(all_free_mice_machines) == 0:
					break
				
				free_mice_machines = [ m for m in all_free_mice_machines if m >= job.machineTypeLowerBound and m < job.machineTypeUpperBound ]
				machineNum = None
				if len(free_mice_machines) == 0:
					machineNum = all_free_mice_machines[0]
				else:	
					machineNum = free_mice_machines[0] 
			 
				print "machineNum : ", machineNum, t.mem, t.cpu
				m = self.cluster.getMachine(machineNum)

				while not m.canAddTask(t, "elephant") :
					taskToKill = m.elephantTaskList[random.randint(0, len(m.elephantTaskList) - 1 )]
					
					m.deleteTask(taskToKill, time, "Preempted")
					j = taskToKill.job
					j.taskPreempted(taskToKill, time)
					ev = j.getTaskDoneEvent( taskToKill )
					q = j.getQueue()
					q.taskEnded(taskToKill, time)
					self.evQueue.remove(ev)
					heapq.heapify(self.evQueue)
					self.preemptedTasks.append(ev)


				m.addTask(t, time, "miceDDRF")
				job.markAsRunning(t, time)  
				queue.taskStarted(t)
				ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
				job.addTaskDoneEvent(t, ev) 
				heapq.heappush( self.evQueue, ev )
				allocated = True
				#print "Added Task : ", job, t , "end time : " , time + t.duration
				if t.machine == None:	
					print "Machine is NONE : ", t.machine
					break
				break

		finish = tm.time()
		print "Time taken : allocateMiceDRF(%d) : \t\t %.2f" %(time, finish-start)


	def allocateUnfinishedElephantDRF(self, time):
		start = tm.time()
		allocated = True
		queue = self.elephantQueue

		freeMachines = self.cluster.getFreeElephantMachineArray() 
		if len(freeMachines[0]) == 0:
			return

		for i in range(len(freeMachines[0])):
			m = self.cluster.getMachine(freeMachines[0][i])
			if len(m.miceJobQueue) > 0:
				continue

			if not m.isMachineFree():
				continue

			if len(queue.jobs) == 0:
				break

			job = queue.getJob()
			if job == None:
				break
			t = job.getReadyTask()
			if not m.canAddTask(t, "elephant"):
				continue				

			m.addTask(t, time , "elephantDRF" )
			job.markAsRunning(t, time)  
			queue.taskStarted(t)
			ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
			job.addTaskDoneEvent(t, ev)
			heapq.heappush( self.evQueue, ev )
			if not job.hasTasksReady():
				print "LP -> midway DRF elephant allocation . job has run out of tasks", job.jobid , "at time : ", time
				self.needNewElephantAllocation = True

			if t.machine == None:	
				print "PANIC. Machine is NONE : ", t.machine
				break


		finish = tm.time()
		print "Time taken : allocateUnfinishedElephantDRF(%d) : \t\t %.2f" %(time, finish-start)

	def allocateElephantDRFPareto(self, time):
		start = tm.time()
		print time, "Pareto Elephant"

		allocated = True
	   	while allocated == True:
			allocated = False
			if len(self.elephantQueue.jobs) == 0:
				break

			freeMachines = self.cluster.getFreeMachineArray()
 			if len(freeMachines[0]) == 0:
				print time, "No more free machines. Elephant Pareto"
				panicflag = False
				for i in range(self.cluster.numMachines):
					if  self.cluster.freeMachines[i] != 0:
						print i , 
						panicflag = True

				print
				if panicflag :
					print time, "PANIC . Elephant Pareto saying no free machine but is wrong"
				break

		
			m = self.cluster.getMachine(freeMachines[0][0])


			job = self.elephantQueue.getJob()
			t = job.getReadyTask()
			allocated = True
			m.addTask(t, time, "elephantDRF" )
			job.markAsRunning(t, time) 
			self.elephantQueue.taskStarted(t)
			ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
			job.addTaskDoneEvent(t, ev)

			heapq.heappush( self.evQueue, ev)
			print time, "Elephant Pareto. Scheduled Job ", job
			if not job.hasTasksReady():
				print "LP -> Pareto elephant allocation . job has run out of tasks" , job.jobid , "at time : ", time
				self.needNewElephantAllocation = True

 
			if t.machine == None:	
				print "PANIC . Machine is NONE : ", t.machine
				break
		finish = tm.time()
		print "Time taken : allocateElephantDRFPareto(%d) : \t\t %.2f" %(time, finish-start)

	def allocateMiceDRFPareto(self, time):
		start = tm.time()

		allocated = True
	   	while allocated == True:
			allocated = False
			if len(self.miceQueue.jobs) == 0:
				break

			freeMachines = self.cluster.getFreeMachineArray()
 			if len(freeMachines[0]) == 0:
				break

		
			m = self.cluster.getMachine(freeMachines[0][0])


			job = self.miceQueue.getJob()
			t = job.getReadyTask()
			allocated = True
			m.addTask(t)
			job.markAsRunning(t, time)  
			self.miceQueue.taskStarted(t)
			ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
			job.addTaskDoneEvent(t, ev)

			heapq.heappush( self.evQueue, ev)


 
			if t.machine == None:	
				print "PANIC . Machine is NONE : ", t.machine
				break
		finish = tm.time()
		print "Time taken : allocateMiceDRFPareto(%d) : \t\t %.2f" %(time, finish-start)



	def allocateElephants(self, time):
		alloc = self.elephantAllocation(time)
		#print "Time : " , time , " , Num current elephants : ", alloc, ", Current elephants : " ,
		#if alloc != None:
		#	for ele in alloc :
		#		print ele ,
		#	print 
		#else:
		#	print "None "
		if alloc == None:
			return
		stoppedMidway = False 

		if self.needNewElephantAllocation == True:
			print >> sys.stderr, " All machine allocation "	
			print "All machine allocation"
			start = tm.time()
			for i in range(self.cluster.numMachines):
				m = self.cluster.getMachine(i)
				if len(m.miceJobQueue) > 0:
					continue
				#if not m.isMachineFree():
				#	continue

				for elephant in sorted(alloc, key=lambda k:self.elephantQueue.getJobById(k).getDomShare()):
					job = self.elephantQueue.getJobById(elephant)

					if job in self.elephantQueue.fullyRunningJobs :
						print "PANIC. Should not happen"
						continue

					t = job.getReadyTask()
					if not m.canAddTask(t, "elephant"):
						continue

					machineType = self.cluster.getMachineType(m)

					numTasks = alloc[elephant][m]
					numTasksAlreadyRunning = m.getNumTasksJob(elephant)
					if numTasksAlreadyRunning >= numTasks:
						continue
					for i in range(numTasksAlreadyRunning, int(numTasks)):
						if not job.hasTasksReady():
							print "LP -> LP allocation. Stopping LP allocation 1. job has run out of tasks" , job.jobid , "at time : ", time
							stoppedMidway = True
							return True
						
						t = job.getReadyTask()
						if m.canAddTask(t , "elephant" ):
							m.addTask(t, time, "DDRF" )
							job.markAsRunning(t, time)
							self.elephantQueue.taskStarted(t)
							ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
							job.addTaskDoneEvent(t, ev)

							heapq.heappush( self.evQueue, ev)


							if job.numTasksRunning > self.perJobAllocation[job.jobid]:
								print "MORETHANREQUIRED - time : %d, jobid : %d, numTasksRunning : %d, LP allocation : %d, numTasksOnMachine LP : %d, numTasksAlreadyRunning ; %d, machineId : %d " % (time, job.jobid, job.numTasksRunning, int(self.perJobAllocation[job.jobid]), numTasks, numTasksAlreadyRunning, m.machineId )

							if t.machine == None:
								print "Machine of ", t, "is None"
							for s in self.statLoggers:
								s.newTask(time, t)
							if not job.hasTasksReady():
								print "LP -> LP allocation. Stopping LP allocation 1. job has run out of tasks" , job.jobid , "at time : ", time
								stoppedMidway = True
								return True
		

							#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
						else:
							#print "Machine", m, "cannot add task"
							break

			end = tm.time()
			print " New LP allocation iterative loop %d took %.2f sec, num players : %d " % (self.numLPInvocations, end - start, len(self.elephantQueue.jobs) )
		else:
			print >> sys.stderr, "Partial machine allocation"
			print "Partial machine allocation. Num task done machines :  ", len(self.taskdoneElephantMachines)
			start = tm.time()
			for m in self.taskdoneElephantMachines:
				if len(m.miceJobQueue) > 0:
					continue

				#if not m.isMachineFree():
				#	continue

				for elephant in sorted(alloc, key=lambda k:self.elephantQueue.getJobById(k).getDomShare()):
					job = self.elephantQueue.getJobById(elephant)
					if job in self.elephantQueue.fullyRunningJobs :
						print "PANIC. job in fully running jobs "
						continue
					
					t = job.getReadyTask()
					if not m.canAddTask(t, "elephant"):
						continue
					machineType = self.cluster.getMachineType(m)
					numTasks = alloc[elephant][m]
					numTasksAlreadyRunning = m.getNumTasksJob(elephant)
					if numTasksAlreadyRunning >= numTasks:
						continue


					for i in range(numTasksAlreadyRunning, int(numTasks)):
						if not job.hasTasksReady():
							print "LP -> LP allocation. Stopping LP allocation 1. job has run out of tasks" , job.jobid , "at time : ", time
							stoppedMidway = True
							return True
						
						t = job.getReadyTask()
						if m.canAddTask(t , "elephant"):
							m.addTask(t, time, "DDRF" )
							job.markAsRunning(t, time)
							self.elephantQueue.taskStarted(t)
							ev = (time + t.duration , Event(time + t.duration, "TASKDONE", t))
							job.addTaskDoneEvent(t, ev)

							heapq.heappush( self.evQueue, ev)


							if job.numTasksRunning > self.perJobAllocation[job.jobid]:
								print "MORETHANREQUIRED - time : %d, jobid : %d, numTasksRunning : %d, LP allocation : %d, numTasksOnMachine LP : %d, numTasksAlreadyRunning ; %d, machineId : %d " % (time, job.jobid, job.numTasksRunning, int(self.perJobAllocation[job.jobid]), numTasks, numTasksAlreadyRunning, m.machineId )

							if t.machine == None:
								print "PANIC : Machine of ", t, "is None"
							for s in self.statLoggers:
								s.newTask(time, t)
							if not job.hasTasksReady():
								print "LP -> LP allocation. Stopping LP allocation 1. job has run out of tasks" , job.jobid , "at time : ", time
								stoppedMidway = True
								return True
		

							#print >> sys.stderr, "pushed taskdone time, duration , time + druation ", time , t.duration , time + t.duration
						else:
							#print "Machine", m, "cannot add task"
							break
			end = tm.time()
			print " Old LP allocation loop %d took %.2f sec, num machines looked into : %d " % (self.numLPInvocations, end - start , len(self.taskdoneElephantMachines) )	


		return stoppedMidway 

	def DRF(self, jobfile, miceQueueSize=sys.maxint ):
		for s in self.statLoggers:
			s.start()


		#setting job threshold for elephants to INFINITY
		mice_gen = EventGenerator( jobfile , sys.maxint, False )   
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, miceQueueSize , mice_gen )


		j = self.miceQueue.readNextJob()
		heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 

		time = None
		while not (len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs or time > self.stopTime):
			time = self.handleEvents()
			if time == None:
				break
			self.allocateMiceDRF(time)

			for s in self.statLoggers:
					s.IterationFinished(time, self.utilStats)


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
		self.elephantQueue = DRFQueue("elephant", None, 10000000, self.cluster, elephantQueueSize , elephant_gen , True )
	
		j = self.miceQueue.readNextJob()
		if j != None:
			heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j))) 
		j = self.elephantQueue.readNextJob()
		if j != None:
			heapq.heappush( self.evQueue, (j.start, Event(j.start, "JOB", j)))

		time = None
		flag = False
		print "Starting simulator"
		print "queue size : ", len(self.evQueue) , "time : ", time, "self.stopTime : ", self.stopTime

		while not (len(self.evQueue) == 0 or self.numJobsCompleted > self.maxJobs or time > self.stopTime ):
			time = self.handleEvents()
			print "queue size : ", len(self.evQueue) , "time : ", time, "self.stopTime : ", self.stopTime
			self.allocateMiceDDRF(time)
			if time == None:
				break
			flag = self.allocateElephants(time)
			if flag == True:
				print "LP -> DRF switch : ", time

				self.allocateUnfinishedElephantDRF(time)
				self.needNewElephantAllocation = True
			else:
				print "LP -> ran without exhausting elephants :" , time
				self.needNewElephantAllocation = False

			#self.allocateElephantDRFPareto(time)
			self.allocateMiceDRFPareto(time)

			for s in self.statLoggers:
					s.IterationFinished(time, self.utilStats)

			self.medianElephantShare = self.elephantQueue.getMedianDominantShare()

		print "Simulation complete"
		self.finish(time)


	def DRF_twice(self, jobfile, miceQueueSize=sys.maxint, elephantQueueSize=sys.maxint):

		for s in self.statLoggers:
			s.start()

		mice_gen = EventGenerator( jobfile , self.elephantTaskThreshold, False )
		self.miceQueue = DRFQueue("mice", DRFSorter(), 10000000, self.cluster, miceQueueSize , mice_gen )

		elephant_gen = EventGenerator( jobfile , self.elephantTaskThreshold , True)
		self.elephantQueue = DRFQueue("elephant", None, 10000000, self.cluster, elephantQueueSize , elephant_gen , True )
	
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
			self.allocateUnfinishedElephantDRF(time)
			self.allocateMiceDRF(time)
			self.allocateElephantDRFPareto(time)
			self.allocateMiceDRFPareto(time)

			for s in self.statLoggers:
					s.IterationFinished(time, self.utilStats)


		print "Simulation complete"
		self.finish(time)

	def finish(self, time):
		for sl in self.statLoggers:
			sl.finish(time)

	def addStatLogger(self, sl):
		self.statLoggers.append(sl)


def start_exp(exp_type, machine_types, fraction_mice_servers, elephant_num_tasks_threshold):

	print "Starting experiment"

	#############################################	
	######		  constants		 ###########
	#############################################

	general = [ 12.0 , 12.0 ]
	mem_optimized = [ 30.0 , 8.0 ]
	cpu_optimized = [ 16 , 30.0 ]

	memHeadroom = 3.0
	cpuHeadroom = 3.0

	jobfile = "toy_input"

	stopTime = 20000
	#maxJobs = 200
	#############################################	
	######	 end  constants		 ###########
	#############################################

	startTime = tm.time()
	tot_machines = 1200
	
	print "Creating machines"

	if machine_types == "G":
		mahcineConfig = [ general ]
		machinesPerType  = [ tot_machines ]
	elif machine_types == "M":
		machineConfig = [ mem_optimized ]
		machinesPerType = [ tot_machines ]
	elif machine_types == "C":
		machineConfig = [ cpu_optimized ]
		machinesPerType = [ tot_machines ]
	elif machine_types == "G-M" :
		machineConfig = [ general , mem_optimized ]
		machinesPerType = [ tot_machines/2 , tot_machines/2 ]
	elif machine_types == "G-C" :
		machineConfig = [ general, cpu_optimized ]
		machinesPerType = [ tot_machines/2 , tot_machines/2 ]
	elif machine_types == "M-C" :
		machineConfig = [ mem_optimized , cpu_optimized ]
		machinesPerType = [ tot_machines/2 , tot_machines/2 ]
	elif machine_types == "G-M-C" :
		machineConfig = [ general, mem_optimized, cpu_optimized ]
		machinesPerType = [ tot_machines/3 , tot_machines/3 , tot_machines/3 ]

	print "Created machines"

	logfile = str(exp_type) + "." +  str(machine_types) + "." +  str(fraction_mice_servers) + "." + str(elephant_num_tasks_threshold)
 
	print "Starting simulation"

	if exp_type == "drf":
		cluster = OneCluster(machineConfig, machinesPerType, cpuHeadroom , memHeadroom , 1.0 )
		sim = Simulator( cluster, sys.maxint )
		sim.setStopTime( stopTime )
		#sim.setMaxJobs( maxJobs )

		sl = JobFinishLogger(logfile + ".finish.csv", cluster)	
		sim.addStatLogger(sl)

		ul = UtilizationLogger(logfile + ".util.csv", cluster)
		sim.addStatLogger(ul)

		jfl = JobFairnessLogger(None, cluster, sim)
		sim.addStatLogger(jfl)

		#ml = MachineUtilLogger(logfile + ".machines.csv" , cluster)
		#sim.addStatLogger(ml)
 
		sim.DRF(jobfile, sys.maxint )

	elif exp_type == "ddrf":
		cluster = OneCluster(machineConfig, machinesPerType, cpuHeadroom , memHeadroom , fraction_mice_servers )
		sim = Simulator( cluster, elephant_num_tasks_threshold )
		sim.setStopTime( stopTime )
		#sim.setMaxJobs( maxJobs )


		sl = JobFinishLogger(logfile + ".finish.csv", cluster)	
		sim.addStatLogger(sl)

		ul = UtilizationLogger(logfile + ".util.csv", cluster)
		sim.addStatLogger(ul)

		jfl = JobFairnessLogger(None, cluster, sim)
		sim.addStatLogger(jfl)


		#ml = MachineUtilLogger(logfile + ".machines.csv" , cluster)
		#sim.addStatLogger(ml)
 
		sim.DDRF( jobfile, sys.maxint, sys.maxint )

	elif exp_type == "drf_twice":
		cluster = OneCluster(machineConfig, machinesPerType, cpuHeadroom , memHeadroom , fraction_mice_servers )
		sim = Simulator( cluster, elephant_num_tasks_threshold )
		#sim.setStopTime( stopTime )
		sim.setMaxJobs( maxJobs )


		sl = JobFinishLogger(logfile + ".finish.csv", cluster)	
		sim.addStatLogger(sl)

		ul = UtilizationLogger(logfile + ".util.csv", cluster)
		sim.addStatLogger(ul)

		jfl = JobFairnessLogger(None, cluster, sim)
		sim.addStatLogger(jfl)


		#ml = MachineUtilLogger(logfile + ".machines.csv" , cluster)
		#sim.addStatLogger(ml)
 
		sim.DRF_twice( jobfile, sys.maxint, sys.maxint )

	finish = tm.time()

	print startTime , finish , "Time taken : " , finish-startTime

   

if __name__== "__main__":
	#sys.settrace(tracefunc)
	
	#signal.signal(signal.SIGINT, signal_handler)
	if len(sys.argv) < 4:
		print "Usage python mainsim.py < <exp_type>  <machine_types> <fraction_mice_servers> <elephant_num_tasks_threshold> "
		exit()

	start_exp(sys.argv[1].strip() , sys.argv[2].strip() , float(sys.argv[3]), int(sys.argv[4]))

