import os
import sys
import numpy
from combinedmachine import *

class OneCluster:

	def __init__(self, machineConfig, machinesPerType, minCpu, minMem, miceFraction ):
		self.numMachines = 0
		self.machines = []
		self.elephantMachinesByType = {} 
		self.cpuUsage = 0
		self.memUsage = 0
		self.totCpu = 0
		self.totMem = 0
		self.machineConfig = machineConfig
		self.machinesPerType = machinesPerType
		self.elephantMachinesPerType = list(machinesPerType)
		self.elephantMachineConfig = list(self.machineConfig)
		self.numMachineTypes = len(self.machinesPerType)
		self.numMachines = 0
	
		self.machineType = {}
		for i in range(len(self.machinesPerType)):
			self.elephantMachinesPerType[i] = machinesPerType[i]

		print "Creating machines "
		print "Number of types of machines : ", machinesPerType
		for i in range(len(machinesPerType)):
			self.numMachines += machinesPerType[i]
			mem = machineConfig[i][0]
			cpu = machineConfig[i][1]
			self.elephantMachinesByType[i] = []
			for j in range( machinesPerType[i] ):
				m = CombinedMachine(cpu, mem, minCpu, minMem, self , miceFraction )
				self.machines.append(m)
				self.elephantMachinesByType[i].append(m)

				self.totCpu += cpu
				self.totMem += mem
				self.machineType[m] = i

		print "Created : ", len(self.machines) , "machines"
		print "Tot mem : " , self.totMem, "Tot cpu : ", self.totCpu

		self.freeMiceMachines = numpy.ones(self.numMachines)
		self.freeElephantMachines = numpy.ones(self.numMachines)
		self.freeMachines = numpy.ones(self.numMachines)

	def getJobSizeThreshold(self):
		return self.jobSizeThreshold

	def getCpuUtil(self):
		return float(cpuUsage) / float(totCpu) 

	def getMemUtil(self):
		return float(memUsage) / float(totMem)

	def getCpuUsage(self):
		return cpuUsage
	
	def getMemUsage(self):
		return memUsage

	def getMachine(self, index):
		return self.machines[index]

	#def getElephantMachine(self, index):
	#	return self.elephantMachines[index]

	def getFreeMiceMachineArray(self):
		return numpy.nonzero(self.freeMiceMachines)

	def getFreeElephantMachineArray(self):
		return numpy.nonzero(self.freeElephantMachines)


	def getFreeMachineArray(self):
		return numpy.nonzero(self.freeMachines)

	def getMachineType(self, m):
		return self.machineType[m]

	def setUtilStats(self, utilStats):
		self.utilStats = utilStats
		utilStats["overall"] = {}
		utilStats["elephants"] = {}
		utilStats["mice"] = {}

		for key in utilStats:
			utilStats[key]["util"] = 0
			utilStats[key]["mem"] = 0
			utilStats[key]["cpu"] = 0
			utilStats[key]["num_tasks"] = 0

		for i in range(self.numMachineTypes):
			utilStats[str(i)] = {}
			utilStats[str(i)]["elephants"] = {}
			utilStats[str(i)]["mice"] = {}

			for key in utilStats[str(i)]:
				utilStats[str(i)][key]["util"] = 0
				utilStats[str(i)][key]["mem"] = 0
				utilStats[str(i)][key]["cpu"] = 0
				utilStats[str(i)][key]["num_tasks"] = 0


