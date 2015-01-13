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
		for i in range(len(self.machinesPerType)):
			self.machinesPerType[i] = machinesPerType[i] - int( miceFraction * machinesPerType[i] )
		self.numMiceMachines = 0
		self.numElephantMachines = 0

		self.elephantMachines = []
		self.miceMachines = []

		print "Creating machines "
		for i in range(len(machinesPerType)):
			self.numMachines += machinesPerType[i]
			mem = machineConfig[i][0]
			cpu = machineConfig[i][1]
			self.elephantMachinesByType[i] = []
			for j in range( machinesPerType[i] ):
				if j < int (float(miceFraction) * float(machinesPerType[i]) ):
					m = MiceMachine(cpu, mem, minCpu, minMem, self , "mice" )
					self.machines.append(m)
					self.miceMachines.append(m)
					self.numMiceMachines += 1

					self.totCpu += cpu
					self.totMem += mem
				else:
					m = ElephantMachine(cpu, mem, minCpu, minMem, self , "elephant" )
					self.machines.append(m)
					self.elephantMachinesByType[i].append(m)
					self.elephantMachines.append(m)
					self.numElephantMachines += 1

					self.totCpu += cpu
					self.totMem += mem

		print "Created : ", len(self.machines) , "machines"
		print "Tot mem : " , self.totMem, "Tot cpu : ", self.totCpu

		self.freeMiceMachines = numpy.zeros(self.numMiceMachines)
		self.freeElephantMachines = numpy.zeros(self.numElephantMachines)

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


