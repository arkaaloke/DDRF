import os
import sys

class Machine:
	stat_id = 0
	def __init__(self, cpu, mem, minCpu, minMem, cluster, jobSizeThreshold, smallJobThreshold, largeJobThreshold ):
		self.cpu = cpu
		self.mem = mem
		self.minCpu = minCpu
		self.minMem = minMem
		self.largeJobThreshold = largeJobThreshold
		self.smallJobThreshold = smallJobThreshold
		self.jobSizeThreshold = jobSizeThreshold

		self.cpuUsage = 0
		self.memUsage = 0
	
		self.largeCpuUsage = 0
		self.largeMemUsage = 0

		self.smallCpuUsage = 0
		self.smallMemUsage = 0

		self.cluster = cluster
		self.machineId = Machine.stat_id
		Machine.stat_id += 1
		self.tasks = 0

		self.tasksByJob = {}
		

		self.cpuUsableByLargeJobs = self.largeJobThreshold * self.cpu
		self.memUsableByLargeJobs = self.largeJobThreshold * self.mem

		self.cpuUsableBySmallJobs = self.smallJobThreshold * self.cpu
		self.memUsableBySmallJobs = self.smallJobThreshold * self.mem


		self.canAddLargeJob = True
		self.canAddSmallJob = True
		self.doesHeadroomExist = True

	def __str__(self):
		return "ID : %s , (%d, %d) " % (self.machineId, self.mem, self.cpu)

	def canAddTask(self, task):

		if task.job.numTasks >= self.jobSizeThreshold:
			if self.cpuUsableByLargeJobs >= self.largeCpuUsage + task.cpu and \
				self.memUsableByLargeJobs >= self.largeMemUsage + task.mem:
				#print "Can add task. machine ID : ", self.machineId
				#print "num tasks in job : ", task.job.numTasks , "Elephant"
				#print "values : (%.2f, %.2f) , (%.2f, %.2f) " % (self.largeJobThreshold * self.cpu, self.largeJobThreshold * self.mem, self.largeCpuUsage + task.cpu, self.largeMemUsage + task.mem)
				return True
			else:
				return False
		 
		if task.job.numTasks < self.jobSizeThreshold:
			if self.cpuUsableBySmallJobs >= self.smallCpuUsage + task.cpu and \
				self.memUsableBySmallJobs >= self.smallMemUsage + task.mem:
				return True
			else:
				return False

		return False
		
		
	def addTask(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.cluster.cpuUsage += task.cpu
		self.cluster.memUsage += task.mem

		if task.job.numTasks >= self.jobSizeThreshold:
			self.largeCpuUsage += task.cpu
			self.largeMemUsage += task.mem
		else:
			self.smallCpuUsage += task.cpu
			self.smallMemUsage += task.mem

		task.machine = self
		self.tasks += 1

		print "MACHINE === Adding task : ", task, "to machine : ", self
		jobid = task.job.jobid
		if jobid not in self.tasksByJob : 
			self.tasksByJob[jobid] = 0

		self.tasksByJob[jobid] += 1
		if self.memUsage > self.mem or self.cpuUsage > self.cpu:
			print "PANIC : MACHINE OVERLOADED ", self , "Usage : (%.2f,%.2f)" % (self.memUsage, self.cpuUsage)
			#print "Large resource usage : (%.2f, %.2f) " % (self.largeMemUsage, self.largeCpuUsage)
			#print "Small resource usage : (%.2f, %.2f) " % (self.smallMemUsage, self.smallCpuUsage) 
			#print "Large job threshold : ", self.largeJobThreshold

	def deleteTask(self, task):
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem

		self.cluster.cpuUsage -= task.cpu
		self.cluster.memUsage -= task.mem

		if task.job.numTasks >= self.jobSizeThreshold:
			self.largeCpuUsage -= task.cpu
			self.largeMemUsage -= task.mem
		else:
			self.smallCpuUsage -= task.cpu
			self.smallMemUsage -= task.mem

		task.machine = None
		self.tasks -= 1

		jobid = task.job.jobid
		self.tasksByJob[jobid] -= 1
		#if len(self.tasksByJob[jobid]) == 0:
		#	del self.tasksByJob[jobid]

	def getMachineId(self):
		return self.machineId

	def getNumTasksJob(self, jobid):
		if jobid not in self.tasksByJob :
			return 0
		else:
			return self.tasksByJob[jobid]


