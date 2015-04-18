import os
import sys
from task import *
import numpy
from job import *

class EventGenerator:
	def __init__(self, filename, taskThreshold , elephantFlag=False):
		self.filename = filename
		self.file = open(filename)
		self.taskThreshold = taskThreshold
		self.elephantFlag = elephantFlag
		self.jobid = 0

	def getNextJob(self):
		#print "EventGenerator : Reading next job"

		starttime = None
		numtasks = None

		while True:
			line = self.file.readline().strip()
			parts = line.split(":")
			if parts[0].strip() != "j":
					print " Something wrong with the input file"
					return None
			starttime = int(parts[1].strip())
			numtasks = int(parts[2].strip())
			self.jobid += 1

			if self.elephantFlag == True:
				if numtasks < self.taskThreshold:
					for i in range(numtasks):
						line = self.file.readline()

				else:
					break
		

			if self.elephantFlag == False:
				if numtasks >= self.taskThreshold:
					for i in range(numtasks):
						line = self.file.readline()
				else:
					break
	
		tasks = []
		memArray = []
		cpuArray = []
		for i in range(numtasks):
			line = self.file.readline() 
			parts = line.strip().split(":")
			if parts[0].strip() != "t":
				exit()
			mem = int(parts[3].strip())
			cpu = float(parts[2]) / float(parts[1])
			duration = int(float(parts[1]) / 1000)

			t = Task(mem, cpu, duration)
			tasks.append(t)

			memArray.append(mem)
			cpuArray.append(cpu)

		avg_mem = numpy.mean(memArray) / (1024 * 1024 *1024)
		avg_cpu = numpy.mean(cpuArray)

		print "READ JOB : " , avg_mem , avg_cpu
		for t in tasks:
			t.mem = avg_mem
			t.cpu = avg_cpu

		j = Job(starttime, tasks, self.jobid)
		return j

