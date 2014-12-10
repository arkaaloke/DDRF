import numpy
import os

class Task:
	def __init__(self, mem, cpu, duration):
		self.mem = mem
		self.cpu = cpu
		self.duration = duration

	
class Job:
	def __init__(self, id, start,  numtasks):
		self.id = id
		self.starttime = start
		self.numtasks = numtasks
		#print "Job : ",id, start, numtasks

	def add_tasks(self, lines):
		self.tasks = []
		for line in lines:
			parts = line.strip().split(":")
			if parts[0].strip() != "t":
				exit()
			mem = int(parts[3].strip())
			cpu = float(parts[2])/ float(parts[1])
			duration = float(parts[1])/1000		

			#print "Task", mem, cpu, duration
			t = Task(mem, cpu, duration)
			self.tasks.append(t)

	def get_num_tasks(self):
		return len(self.tasks)

	def get_agg_mem(self):
		mem = []
		for task in self.tasks:
			mem.append(task.mem)

		return (numpy.mean(mem), numpy.std(mem))
	
	def get_agg_cpu(self):
		cpu = []
		for task in self.tasks:
			cpu.append(task.cpu)
		
		return (numpy.mean(cpu), numpy.std(cpu))

	def get_duration(self):
		duration = 0
		for task in self.tasks:
			if task.duration > duration:
				duration = task.duration

		return duration

			
def read_trace(filename):
	jobs = [] # using systems terminology
	lines = open(filename).readlines()
	numlines = len(lines)
	jobid = 1
	lineno = 0
	while lineno < numlines:
		parts = lines[lineno].strip().split(":")
		if parts[0].strip() != "j":
			print " Something wrong with the input file"
			exit()
		starttime = int(parts[1].strip())
		numtasks = int(parts[2].strip())
	
		j = Job(jobid, starttime, numtasks)
		j.add_tasks(lines[lineno + 1: lineno + numtasks + 1])
		jobs.append(j)
		jobid += 1
		lineno += (numtasks+1)

	print "Finished readng jobs "
	for job in jobs :
		(avgmem, stdmem) = job.get_agg_mem()
		(avgcpu, stdcpu) = job.get_agg_cpu()
		print job.id, job.numtasks, int( avgmem / (1024*1024)) , int( stdmem / (1024*1024)), job.get_agg_cpu()[0], job.get_duration()
	return jobs

if __name__ == "__main__":
	read_trace("sorted_jobs-exp-small")

