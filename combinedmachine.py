import os
import sys
from basicmachine import *

class CombinedMachine():
	ele_stat_id = 0
	mice_stat_id = 0
	machine_id = 0
	def __init__(self, cpu, mem, minCpu, minMem, cluster, miceFraction ):
		self.cpu = cpu
		self.mem = mem
		self.minCpu = minCpu
		self.minMem = minMem
		
		self.cpuUsage = 0
		self.memUsage = 0
		self.miceCpuUsage = 0
		self.miceMemUsage = 0
		self.elephantCpuUsage = 0
		self.elephantMemUsage = 0

		self.miceFraction = miceFraction

		self.cluster = cluster
		self.machineId = CombinedMachine.ele_stat_id
		CombinedMachine.ele_stat_id += 1

		self.tasks = 0

		self.tasksByJob = {}
		
		self.isFree = True

	def canAddTask(self, task, jobType=None):
		if not self.isFree:
			return False

		if jobType == None:
			return self.isFree

		elif jobType == "elephant":
			if self.elephantMemUsage + task.mem <= self.mem * ( 1 - self.miceFraction ) and self.elephantCpuUsage + task.cpu <= self.cpu * ( 1 - self.miceFraction ):
				return True
			else:
				return False
		elif jobType == "mice":
			if self.cluster.freeMiceMachines[self.machineId] == 0:
				return False
			else:
				return True

	def addTask(self, task):
		self.cpuUsage += task.cpu
		self.memUsage += task.mem

		self.cluster.cpuUsage += task.cpu
		self.cluster.memUsage += task.mem

		task.machine = self
		self.tasks += 1
		
		self.cluster.utilStats["overall"]["cpu"] += task.cpu
		self.cluster.utilStats["overall"]["mem"] += task.mem
		self.cluster.utilStats["overall"]["num_tasks"] += 1

		if task.job.isElephant():
			self.elephantCpuUsage += task.cpu
			self.elephantMemUsage += task.mem

			self.cluster.utilStats["elephants"]["cpu"] += task.cpu
			self.cluster.utilStats["elephants"]["mem"] += task.mem
			self.cluster.utilStats["elephants"]["num_tasks"] += 1
			
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["cpu"] += task.cpu
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["mem"] += task.mem
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["num_tasks"] += 1
		else:
			self.miceCpuUsage += task.cpu
			self.miceMemUsage += task.mem
			self.cluster.utilStats["mice"]["cpu"] += task.cpu
			self.cluster.utilStats["mice"]["mem"] += task.mem
			self.cluster.utilStats["mice"]["num_tasks"] += 1
			
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["cpu"] += task.cpu
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["mem"] += task.mem
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["num_tasks"] += 1


		jobid = task.job.jobid
		if jobid not in self.tasksByJob : 
			self.tasksByJob[jobid] = 0

		self.tasksByJob[jobid] += 1

		if self.memUsage > self.mem or self.cpuUsage > self.cpu:
			print "PANIC : MACHINE OVERLOADED ", self , "Usage : (%.2f,%.2f)" % (self.memUsage, self.cpuUsage)


		if self.cluster.freeElephantMachines[self.machineId] == 1:
			# not fit for elephants
			if self.elephantMemUsage >= ( 1 - self.miceFraction) * self.mem or self.elephantCpuUsage >= ( 1 - self.miceFraction ) * self.cpu:
				self.cluster.freeElephantMachines[self.machineId] = 0


		if self.cluster.freeMiceMachines[self.machineId] == 1:
			# not fit for mice anymore
			if self.miceMemUsage + self.minMem >= (self.miceFraction * self.mem) or self.miceCpuUsage + self.minCpu >= ( self.miceFraction * self.cpu) :
				#print "CHANGING MICE MACHINE STATE for machine : " , self.machineId , 
				#print self.miceMemUsage , self.minMem , self.miceFraction , self.mem , self.miceCpuUsage, self.minCpu , self.miceFraction , self.cpu
				self.cluster.freeMiceMachines[self.machineId] = 0


		if self.cluster.freeMachines[self.machineId] == 1:
			# if overall machine is overloaded
			if self.memUsage + self.minMem >= self.mem or self.cpuUsage + self.minCpu >= self.cpu:
				self.isFree = False
				self.cluster.freeMachines[self.machineId] = 0

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

		if task.job.isElephant():
			self.elephantCpuUsage -= task.cpu
			self.elephantMemUsage -= task.mem

			self.cluster.utilStats["elephants"]["cpu"] -= task.cpu
			self.cluster.utilStats["elephants"]["mem"] -= task.mem
			self.cluster.utilStats["elephants"]["num_tasks"] -= 1
			
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["cpu"] -= task.cpu
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["mem"] -= task.mem
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["elephants"]["num_tasks"] -= 1
		else:
			self.miceCpuUsage -= task.cpu
			self.miceMemUsage -= task.mem

			self.cluster.utilStats["mice"]["cpu"] -= task.cpu
			self.cluster.utilStats["mice"]["mem"] -= task.mem
			self.cluster.utilStats["mice"]["num_tasks"] -= 1
			
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["cpu"] -= task.cpu
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["mem"] -= task.mem
			self.cluster.utilStats[str(self.cluster.getMachineType(self))]["mice"]["num_tasks"] -= 1

		jobid = task.job.jobid
		self.tasksByJob[jobid] -= 1

		if self.cluster.freeElephantMachines[self.machineId] == 0:
			# not fit for elephants
			if self.elephantMemUsage < ( 1 - self.miceFraction) * self.mem and self.elephantCpuUsage < ( 1 - self.miceFraction ) * self.cpu:
				self.cluster.freeElephantMachines[self.machineId] = 1


		if self.cluster.freeMiceMachines[self.machineId] == 0:
			# not fit for mice anymore
			if self.miceMemUsage + self.minMem < (self.miceFraction * self.mem) and self.miceCpuUsage + self.minCpu < ( self.miceFraction * self.cpu) :
				self.cluster.freeMiceMachines[self.machineId] = 1


		if self.cluster.freeMachines[self.machineId] == 0:
			# if overall machine is overloaded
			if self.memUsage + self.minMem < self.mem and self.cpuUsage + self.minCpu < self.cpu:
				self.isFree = True
				self.cluster.freeMachines[self.machineId] = 1


	def getMachineId(self):
		return self.machineId

	def getNumTasksJob(self, jobid):
		if jobid not in self.tasksByJob :
			return 0
		else:
			return self.tasksByJob[jobid]


	def isMachineFree(self):
		return self.isFree
	
