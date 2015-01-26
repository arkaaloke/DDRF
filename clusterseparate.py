import os
import sys
from micemachine import *
from elephantmachine import *
import numpy

class ClusterSeparate:

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
	
		# subtract headroom 
		#for i in range(len(self.elephantMachineConfig)) :
		#	self.elephantMachineConfig[i][0] -= minMem
		#	self.elephantMachineConfig[i][1] -= minCpu

		self.machineType = {}
		for i in range(len(self.machinesPerType)):
			self.elephantMachinesPerType[i] = machinesPerType[i] - int( miceFraction * machinesPerType[i] )
		self.numMiceMachines = 0
		self.numElephantMachines = 0

		self.elephantMachines = []
		self.miceMachines = []

		print "Creating machines "
		print "Number of types of machines : ", machinesPerType
		for i in range(len(machinesPerType)):
			self.numMachines += machinesPerType[i]
			mem = machineConfig[i][0]
			cpu = machineConfig[i][1]
			self.elephantMachinesByType[i] = []
			for j in range( machinesPerType[i] ):
				#print "Number of mice machines : " , float(miceFraction) , float(machinesPerType[i]) , int (float(miceFraction) * float(machinesPerType[i]) ) 
				if j < int (float(miceFraction) * float(machinesPerType[i]) ):
					m = MiceMachine(cpu, mem, minCpu, minMem, self , "mice" )
					self.machines.append(m)
					self.miceMachines.append(m)
					self.numMiceMachines += 1

					self.totCpu += cpu
					self.totMem += mem
					self.machineType[m] = i
				else:
					m = ElephantMachine(cpu, mem, minCpu, minMem, self , "elephant" )
					self.machines.append(m)
					self.elephantMachinesByType[i].append(m)
					self.elephantMachines.append(m)
					self.numElephantMachines += 1

					self.totCpu += cpu
					self.totMem += mem
					self.machineType[m] = i

		print "Created : ", len(self.machines) , "machines"
		print "Tot mem : " , self.totMem, "Tot cpu : ", self.totCpu
		print "Created : ", self.numMiceMachines, " mice. ", self.numElephantMachines , " elephants . Machines"
		self.freeMiceMachines = numpy.ones(self.numMiceMachines)
		self.freeElephantMachines = numpy.ones(self.numElephantMachines)

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

	def getMiceMachine(self, index):
		return self.miceMachines[index]

	def getElephantMachine(self, index):
		return self.elephantMachines[index]

	def getFreeMiceMachineArray(self):
		return numpy.nonzero(self.freeMiceMachines)

	def getFreeElephantMachineArray(self):
		return numpy.nonzero(self.freeElephantMachines)


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


