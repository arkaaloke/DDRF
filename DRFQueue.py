import os
import sys
from Queue import *
from jobqueue import *
import heapq

class DRFQueue(JobQueue):

	def addJob(self, job):
		job.setClusterParams(self.cluster.totCpu, self.cluster.totMem)
		#print "QUEUE : CLUSTER PARAMS : ", self.cluster.totCpu, self.cluster.totMem  
		self.jobs.append(job)
		print "Length of jobs queue : ", len(self.jobs)	
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

	def getJob(self):
		if len(self.jobs) == 0:
			return None

		#return self.jobs[0]

		job = min(self.jobs, key=lambda item: item.getDomShare())

		#job = heapq.heappop(self.jobs)
		#heapq.heappush(self.jobs, job)
		return job

		#print "NUMBER OF JOBS : ", len(self.jobs)
		#print "SORTED ORDER :"
		#for i in range(len(self.jobs)):
		#	print self.jobs[i]
		#j = self.jobs[0]
		#return j

	def taskEnded(self, task, time):
		self.memUsage -= task.mem
		self.cpuUsage -= task.cpu
		self.numRunningTasks -= 1
		job = task.job

		#if job in self.jobs:
		#	self.jobs.remove(job)
		#	heapq.heappush(self.jobs, job)


