import os
import sys
from basicmachine import *

class ElephantMachine(BasicMachine):

	
	def canAddTask(self, task):
		return self.isFree
	

	def addTask(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.cluster.cpuUsage += task.cpu
		self.cluster.memUsage += task.mem

		task.machine = self
		self.tasks += 1

		jobid = task.job.jobid
		if jobid not in self.tasksByJob : 
			self.tasksByJob[jobid] = 0

		self.tasksByJob[jobid] += 1
		if self.memUsage > self.mem or self.cpuUsage > self.cpu:
			print "PANIC : MACHINE OVERLOADED ", self , "Usage : (%.2f,%.2f)" % (self.memUsage, self.cpuUsage)


		if self.mem - self.memUsage < self.minMem or self.cpu - self.cpuUsage < self.minCpu:
			self.isFree = False
 			self.cluster.freeElephantMachines[self.machineId] = 1


	def deleteTask(self, task):
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem

		self.cluster.cpuUsage -= task.cpu
		self.cluster.memUsage -= task.mem

		task.machine = None
		self.tasks -= 1

		jobid = task.job.jobid
		self.tasksByJob[jobid] -= 1
		#if len(self.tasksByJob[jobid]) == 0:
		#	del self.tasksByJob[jobid]

		if not self.isFree:
			if self.mem - self.memUsage > self.minMem and self.cpu - self.cpuUsage > self.minCpu:
				self.isFree = True
				self.cluster.freeElephantMachines[self.machineId] = 0

	
