import os
import sys

class Machine:
	stat_id = 0
	def __init__(self, cpu, mem, minCpu, minMem, cluster, jobSizeThreshold, largeJobThreshold, smallJobThreshold ):
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
		self.tasks = []

	def __str__(self):
		return "ID : %s , (%d, %d) " % (self.machineId, self.mem, self.cpu)
	def canAddTask(self, task):
		if task.job.numTasks >= self.jobSizeThreshold and \
			self.largeJobThreshold * self.cpu >= self.largeCpuUsage + task.cpu and \
			self.largeJobThreshold * self.mem >= self.largeMemUsage + task.mem:
			return True
		 
		if task.job.numTasks < self.jobSizeThreshold and \
			self.smallJobThreshold * self.cpu >= self.smallCpuUsage + task.cpu and \
			self.smallJobThreshold * self.mem >= self.smallMemUsage + task.mem:
			return True

		return False
		
		
	def addTask(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.cluster.cpuUsage += task.cpu
		self.cluster.memUsage += task.mem

		if task.job.numTasks >= self.largeJobThreshold:
			self.largeCpuUsage += task.cpu
			self.largeMemUsage += task.mem
		else:
			self.smallCpuUsage += task.cpu
			self.smallMemUsage += task.mem

		task.machine = self
		self.tasks.append(task)

	def deleteTask(self, task):
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem

		self.cluster.cpuUsage -= task.cpu
		self.cluster.memUsage -= task.mem

		if task.job.numTasks >= self.largeJobThreshold:
			self.largeCpuUsage -= task.cpu
			self.largeMemUsage -= task.mem
		else:
			self.smallCpuUsage -= task.cpu
			self.smallMemUsage -= task.mem

		task.machine = None
		self.tasks.remove(task)

	def getMachineId(self):
		return self.machineId

	def getNumTasksJob(self, jobid):
		count = 0
		for t in self.tasks:
			if t.job.jobid == jobid:
				count += 1

		return count
