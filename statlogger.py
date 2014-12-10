import os
import sys

class StatLogger:
	def __init__(self, filename):
		self.filename = filename
		self.f = open(filename, "w")

	def event(self, time, ev):
		pass

	def newJob(self, time, job):
		pass
	def newTask(self, time, task):
		pass

	def taskDone(self, time, task):
		pass

	def jobDone(self, time, job):
		pass
	def start(self):
		pass
	
	def IterationFinished(self, time):
		pass
	def finish(self, time):
		pass

class JobFinishLogger(StatLogger):
	def start(self):
		self.f.write("JOBID, NUMTASKS, MEM, CPU, STARTTIME, ACTUAL STARTTIME, COMPLETION TIME, DELAY\n")
	def jobDone(self, time, job):
		line = " %d, %d, %.2f, %.2f, %d, %d, %d, %d \n"
		self.f.write( line % (job.jobid, job.numTasks, job.mem, job.cpu, job.start, job.actualStartTime, time, time - job.start) )

 	def finish(self, time):
		self.f.close()

