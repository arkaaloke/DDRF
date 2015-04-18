import os
import sys
from Queue import *
from jobqueue import *
import heapq
import numpy

class DRFQueue(JobQueue):

	def addJob(self, job):
		job.setClusterParams(self.cluster.totCpu, self.cluster.totMem, self.cluster.elephantMachinesByType )
		#print "QUEUE : CLUSTER PARAMS : ", self.cluster.totCpu, self.cluster.totMem  
		self.jobs.append(job)
		print self.name , "Length of jobs queue : ", len(self.jobs)	
		self.hasReadyTasks = True
		self.numTasks += job.numTasks
		print "Added Job : ", job, "to queue : ", self.name


	def taskStarted(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.numRunningTasks += 1
		job = task.job


		if job.allTasksAllocated():
			print "Removing job because all tasks are allocated " , job
			self.jobs.remove(job)
			self.fullyRunningJobs.append(job)

	def getMedianDominantShare(self):
		if len(self.jobs) == 0:
			return sys.maxint
		medianShare = numpy.median([ job.getDomShare() for job in self.jobs ])
		return medianShare

	def getJob(self, machineType=None):
		if len(self.jobs) == 0:
			return None

		if machineType == None:
			job = min(self.jobs, key=lambda item: item.getDomShare())
		else:
			requiredJobs = [ job for job in self.jobs if job.bestMachineType == machineType ]
			if len(requiredJobs) == 0:
				return None
			job = min( requiredJobs , key=lambda item:item.getDomShare())
		
		return job

	def taskEnded(self, task, time):
		self.memUsage -= task.mem
		self.cpuUsage -= task.cpu
		self.numRunningTasks -= 1
		job = task.job

		#if job in self.jobs:
		#	self.jobs.remove(job)
		#	heapq.heappush(self.jobs, job)


	def getJobById(self, jobid):
		for j in self.jobs:
			if j.jobid == jobid:
				return j

		for j in self.fullyRunningJobs:
			if j.jobid == jobid:
				return j
		print "JOBID : ", jobid, " does not exist"
		#print "queue : ", self.name , " jobs : " ,
		#for i in range(len(self.jobs)):
		#	print self.jobs[i].jobid,
		#print
		return None
