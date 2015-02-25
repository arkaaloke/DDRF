import os
import sys

class StatLogger:
	def __init__(self, filename, cluster):
		self.filename = filename
		self.f = open(filename, "w")
		self.cluster = cluster

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
	
	def IterationFinished(self, time, utilStats=None):
		pass

	def finish(self, time):
		pass

class MachineUtilLogger(StatLogger):
	def start(self):
	 	for i in range(self.cluster.numMachines):
			self.f.write("Time, Total_" + str(i) + "_util, Total_" + str(i) + "_cpu, Total_" + str(i) + "_mem,")
		 
	   	for i in range(self.cluster.numMachines):
			self.f.write("E_" + str(i) + "_util, E_" + str(i) + "_cpu, E_" + str(i) + "_mem,")
		 

		for i in range(self.cluster.numMachines):
			self.f.write("M_" + str(i) + "_util, M_" + str(i) + "_cpu, M_" + str(i) + "_mem,")
 
		self.f.write("\n")
		self.f.flush()


	def IterationFinished(self,time, param=None):

		self.f.write(str(time) + "," )
		for i in range(self.cluster.numMachines):
			
			self.f.write(  str( max( self.cluster.machines[i].cpuUsage / self.cluster.machines[i].cpu , self.cluster.machines[i].memUsage / self.cluster.machines[i].mem) ) + "," \
						 + str( self.cluster.machines[i].cpuUsage ) +  "," \
						 + str( self.cluster.machines[i].memUsage ) +  "," )
			
		for i in range(self.cluster.numMachines):
			
			self.f.write(  str( max( self.cluster.machines[i].elephantCpuUsage / self.cluster.machines[i].cpu , self.cluster.machines[i].elephantMemUsage / self.cluster.machines[i].mem) ) + "," \
						 + str( self.cluster.machines[i].elephantCpuUsage ) +  "," \
						 + str( self.cluster.machines[i].elephantMemUsage ) +  "," )
				
		for i in range(self.cluster.numMachines):
			
			self.f.write(  str( max( self.cluster.machines[i].miceCpuUsage / self.cluster.machines[i].cpu , self.cluster.machines[i].miceMemUsage / self.cluster.machines[i].mem) ) + "," \
						 + str( self.cluster.machines[i].miceCpuUsage ) +  "," \
						 + str( self.cluster.machines[i].miceMemUsage ) +  "," )
									

		self.f.write("\n") 
		self.f.flush()

	
 	def finish(self, time):
		self.f.close()


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

class UtilizationLogger(StatLogger):

	def start(self):
		self.num_machine_types = self.cluster.numMachineTypes

		self.f.write("Time, \
					Overall_util, Overall_cpu, Overall_mem, Overall_num_tasks, \
					E_overall_util, E_overall_cpu, E_overall_mem, E_overall_num_tasks, \
					M_overall_util, M_overall_cpu, M_overall_mem, M_overall_num_tasks, ")

		for i in range(self.num_machine_types):
			self.f.write("E_" + str(i) + "_util, E_" + str(i) + "_cpu, E_" + str(i) + "_mem, E_" + str(i) + "_num_tasks")
		 

		for i in range(self.num_machine_types):
			self.f.write("M_" + str(i) + "_util, M_" + str(i) + "_cpu, M_" + str(i) + "_mem, M_" + str(i) + "_num_tasks")
 
		self.f.write("\n")
		self.f.flush()

	def IterationFinished(self, time, E_stats=None):
		line_overall = " %d," \
					   "%.2f, %.2f, %.2f, %d," \
					   "%.2f, %.2f, %.2f, %d," \
					   "%.2f, %.2f, %.2f, %d,"
 
		self.f.write( line_overall % (time , \
						E_stats["overall"]["util"], E_stats["overall"]["cpu"], E_stats["overall"]["mem"], E_stats["overall"]["num_tasks"], 
						E_stats["elephants"]["util"] , E_stats["elephants"]["cpu"] ,E_stats["elephants"]["mem"] ,E_stats["elephants"]["num_tasks"] ,
						E_stats["mice"]["util"] , E_stats["mice"]["cpu"] ,E_stats["mice"]["mem"] ,E_stats["mice"]["num_tasks"] ) )

		for i in range(self.num_machine_types):
			self.f.write(  str(E_stats[str(i)]["elephants"]["util"]) + "," \
						 + str(E_stats[str(i)]["elephants"]["cpu"]) +  "," \
						 + str(E_stats[str(i)]["elephants"]["mem"]) +  "," \
						 + str(E_stats[str(i)]["elephants"]["num_tasks"]) + "," )
						
		for i in range( self.num_machine_types):
			self.f.write(  str(E_stats[str(i)]["mice"]["util"]) + "," \
						 + str(E_stats[str(i)]["mice"]["cpu"]) +  "," \
						 + str(E_stats[str(i)]["mice"]["mem"]) +  "," \
						 + str(E_stats[str(i)]["mice"]["num_tasks"]) + "," )

		self.f.write("\n") 
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


