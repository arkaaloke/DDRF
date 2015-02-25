import os
import sys
from Queue import *
import heapq

class JobQueue:
	def __init__(self, name, sched, maxJobs, cluster, admissionThreshold , gen, elephantStatus=False):
		self.cpuUsage = 0
		self.memUsage = 0
		
		self.admissionThreshold = admissionThreshold
		self.numTasks = 0
		self.numRunningTasks = 0
		self.numCompletedJobs = 0
		self.name = name
		self.sched = sched
		self.cluster = cluster
		self.jobs = []
		self.gen = gen
		self.hasReadyTasks = True
		self.numJobsCompleted = 0
		self.elephantStatus = elephantStatus
		self.fullyRunningJobs = []
		self.waitList = []

	def getElephantStatus(self):
		return self.elephantStatus

	def getDomShare(self):
		return max( float(self.cpuUsage) / float(cluster.totCpu) , float(self.memUsage) / float(cluster.totMem) )

	def readNextJob(self):
		if len(self.waitList) == 0 :
			print self.name, "queue waitlist empty"
			j = self.gen.getNextJob()
			if j==None:
				return None
			j.queue = self
			#print "Read job : ", j
			if len(self.jobs) >= self.admissionThreshold:
				self.waitList.append(j)
				#print "Added job to waitlist "
				return None
			else:
				return j
	
	def getWaitlistedJob(self):
		if len(self.waitList) > 0:
			print self.name, "queue waitlist not empty"
			j = self.waitList[0]
			self.waitList.remove(j)
			print "Read job : ", j
			return j
		else:
			return None

	def getMemUsage(self):
		return self.memUsage

	def getCpuUsage(self):
		return self.cpuUsage

	def addJob(self, job):
		pass

	def jobCompleted(self, job):
		self.numTasks -= job.numTasks
		if len(self.jobs) == 0 :
			self.hasReadyTasks = False

		self.numJobsCompleted += 1
		self.fullyRunningJobs.remove(job)

	def getQueueName(self):
		return self.name

	def taskStarted(self, task):
		pass

	def hasWaitlistedJobs(self):
		if len(self.waitList) > 0 :
			print >> sys.stderr, "\n\n", self.name,  "Waitlist length : ", len(self.waitList), "\n\n" 
			return True
		else:
			return False

	def taskEnded(self, task, time):
		pass


