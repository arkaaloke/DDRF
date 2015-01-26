import os
import sys
from basicmachine import *

class MiceMachine(BasicMachine):

	def canAddTask(self, task):
		return self.isFree
		
		
	def addTask(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.cluster.cpuUsage += task.cpu
		self.cluster.memUsage += task.mem

		task.machine = self
		self.tasks += 1

		#print self.cluster.utilStats

		self.cluster.utilStats["overall"]["cpu"] += task.cpu
		self.cluster.utilStats["overall"]["mem"] += task.mem
		self.cluster.utilStats["overall"]["num_tasks"] += 1

		self.cluster.utilStats["mice"]["cpu"] += task.cpu
		self.cluster.utilStats["mice"]["mem"] += task.mem
		self.cluster.utilStats["mice"]["num_tasks"] += 1
		
		#print "machine type : ", str(self.cluster.getMachineType(self))
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["cpu"] += task.cpu
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["mem"] += task.mem
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["num_tasks"] += 1

		jobid = task.job.jobid
		if jobid not in self.tasksByJob : 
			self.tasksByJob[jobid] = 0

		self.tasksByJob[jobid] += 1

		if self.memUsage > self.mem or self.cpuUsage > self.cpu:
			print "PANIC : MACHINE OVERLOADED ", self , "Usage : (%.2f,%.2f)" % (self.memUsage, self.cpuUsage)

		if self.mem - self.memUsage < self.minMem or self.cpu - self.cpuUsage < self.minCpu:
			self.isFree = False
 			self.cluster.freeMiceMachines[self.machineId] = 0

	def deleteTask(self, task):
		self.cpuUsage -= task.cpu
		self.memUsage -= task.mem

		self.cluster.cpuUsage -= task.cpu
		self.cluster.memUsage -= task.mem

		task.machine = None
		self.tasks -= 1
		
		self.cluster.utilStats["overall"]["cpu"] -= task.cpu
		self.cluster.utilStats["overall"]["mem"] -= task.mem
		self.cluster.utilStats["overall"]["num_tasks"] -= 1

		self.cluster.utilStats["mice"]["cpu"] -= task.cpu
		self.cluster.utilStats["mice"]["mem"] -= task.mem
		self.cluster.utilStats["mice"]["num_tasks"] -= 1
		
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["cpu"] -= task.cpu
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["mem"] -= task.mem
		self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["num_tasks"] -= 1


		jobid = task.job.jobid
		self.tasksByJob[jobid] -= 1

		if not self.isFree:
			if self.mem - self.memUsage > self.minMem and self.cpu - self.cpuUsage > self.minCpu:
				self.isFree = True
				self.cluster.freeMiceMachines[self.machineId] = 1

			
