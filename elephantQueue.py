import os
import sys
from Queue import *
from jobqueue import *

class ElephantQueue(JobQueue):

	def addJob(self, job):
		job.setClusterParams(self.cluster.totCpu, self.cluster.totMem)
		#print "QUEUE : CLUSTER PARAMS : ", self.cluster.totCpu, self.cluster.totMem  
		self.jobs.append(job)
		print "Length of jobs queue : ", len(self.jobs)	
		self.hasReadyTasks = True
		self.numTasks += job.numTasks
		print "Added Job : ", job, "to queue : ", self.name


	def getJob(self):
		j = self.jobs[0]
		return j



	def taskStarted(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.numRunningTasks += 1
		job = task.job


		if job.allTasksAllocated():
			print "Removing job because all tasks are allocated " , job
			self.jobs.remove(job)
			self.fullyRunningJobs.append(job)

	def taskEnded(self, task, time):
		self.memUsage -= task.mem
		self.cpuUsage -= task.cpu
		self.numRunningTasks -= 1
		job = task.job


	def getJobById(self, jobid):
		for j in self.jobs:
			if j.jobid == jobid:
				return j

		for j in self.fullyRunningJobs:
			if j.jobid == jobid:
				return j
		return None
