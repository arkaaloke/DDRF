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
		self.f.flush()

	def jobDone(self, time, job):
		line = " %d, %d, %.2f, %.2f, %d, %d, %d, %d \n"
		self.f.write( line % (job.jobid, job.numTasks, job.mem, job.cpu, job.start, job.actualStartTime, time, time - job.start) )
		self.f.flush()
 	def finish(self, time):
		self.f.close()

class ElephantLogger(StatLogger):
	def start(self):
		self.f.write("ELEPHANT TASKS\n\n")

	def jobDone(self, time, job):
		if not job.isElephant():
			return
		line = "JOB DONE: %d, %d, %.2f, %.2f, %d, %d, %d, %d \n"
		self.f.write( line % (job.jobid, job.numTasks, job.mem, job.cpu, job.start, job.actualStartTime, time, time - job.start) )
		self.f.flush()

	def newTask(self, time, task):
		if not task.job.isElephant():
			return
		self.f.write("NEW TASK : " + str(time) + " JOB : " + str(task.job) + " TASK : " + str(task) + "\n")
		print "NEW TASK : " + str(time) + " JOB : " + str(task.job) + " TASK : " + str(task) + "\n"
		self.f.flush()

	def taskDone(self, time, task):
		if not task.job.isElephant():
			return
		self.f.write("TASKDONE : " + str(time) + " JOB : " + str(task.job) + " TASK : " + str(task) + "\n")
		print "TASKDONE : " + str(time) + " JOB : " + str(task.job) + " TASK : " + str(task) + "\n"
		self.f.flush()


