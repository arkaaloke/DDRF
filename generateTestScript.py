import os
import random
import sys

class EventGenerator:
	def __init__(self, filename, taskThreshold , outputfile, elephantFlag=False):
		self.filename = filename
		self.file = open(filename)
		self.taskThreshold = taskThreshold
		self.elephantFlag = elephantFlag
		self.jobid = 0
		self.outputfile = open(outputfile, "w")

	def getAllJobs(self):
		#print "EventGenerator : Reading next job"

		starttime = None
		numtasks = None

		numJobs = 0
		while True:
		
			line = self.file.readline().strip()
			parts = line.split(":")
			if parts[0].strip() != "j":
					print " Something wrong with the input file"
					return None
			starttime = int(parts[1].strip())
			numtasks = int(parts[2].strip())
			self.jobid += 1
	
			if starttime > 100000:
				break
			if self.elephantFlag == True:
				if numtasks < self.taskThreshold:
					for i in range(numtasks):
						line = self.file.readline()
					continue
	   
			numJobs += 1
			if numJobs % 100 == 0:
				print "Done : ", numJobs
			tasks = []
			memArray = []
			cpuArray = []
			for i in range(numtasks):
				line = self.file.readline() 
				parts = line.strip().split(":")
				if parts[0].strip() != "t":
					print "Wrong format input file"
					exit()
				mem = int(parts[3].strip())
				cpu = float(parts[2]) / float(parts[1])
				duration = int(float(parts[1]) / 1000)

				tasks.append(duration)

			memArray.append(mem)
			cpuArray.append(cpu)

			avg_mem = 0.0
			avg_cpu = 0.0 

			randNum = random.random()
			self.outputfile.write("j:" + str(starttime) + ":" + str(numtasks) + "\n")
			for t in tasks:
				if randNum < 0.5:
					avg_mem = ( 3 * 1024 * 1024 *1024)
					avg_cpu = t * 1000 
				else:
					avg_mem = ( 1 * 1024 * 1024 *1024)
					avg_cpu = t * 1000 * 3

				self.outputfile.write( "t:" + str(t * 1000) + ":" + str(avg_cpu) + ":" + str(avg_mem) + "\n")



def main():
	ev = EventGenerator( "../../sorted_jobs-exp", 5000 , "toy_input" , True)
	ev.getAllJobs()

if __name__ == "__main__":
	main()
