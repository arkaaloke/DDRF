import os
import sys
from machine import *

class Cluster:
	def __init__(self, machineConfig, machinesPerType, minCpu, minMem, jobSizeThreshold , smallJobThreshold, largeJobThreshold ):
		self.numMachines = 0
		self.machines = []
		self.machinesByType = {} 
		self.cpuUsage = 0
		self.memUsage = 0
		self.totCpu = 0
		self.totMem = 0
		self.jobSizeThreshold = jobSizeThreshold
		self.machineConfig = machineConfig
		self.machinesPerType = machinesPerType

		print "Creating machines "
		for i in range(len(machinesPerType)):
			self.numMachines += machinesPerType[i]
			mem = machineConfig[i][0]
			cpu = machineConfig[i][1]
			self.machinesByType[i] = []
			for j in range( machinesPerType[i] ):
				m = Machine(cpu, mem, minCpu, minMem, self , jobSizeThreshold, smallJobThreshold, largeJobThreshold )

				self.machines.append(m)
				self.machinesByType[i].append(m)
				self.totCpu += cpu
				self.totMem += mem

		print "Created : ", len(self.machines) , "machines"
		print "Tot mem : " , self.totMem, "Tot cpu : ", self.totCpu

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


