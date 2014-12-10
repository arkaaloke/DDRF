import numpy 
import functions
import CDRF
import measurePerformance
import IterativeDRF
import cPickle as pickle
import random

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

			
def read_trace(filename, numplayers):
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
		
		if numtasks > 100:
			lineno += (numtasks+1)
			continue	
		j = Job(jobid, starttime, numtasks)
		j.add_tasks(lines[lineno + 1: lineno + numtasks + 1])
		jobs.append(j)
		jobid += 1
		lineno += (numtasks+1)
		if jobid > numplayers:
			break

		
	print "Finished readng jobs "
	for job in jobs :
		(avgmem, stdmem) = job.get_agg_mem()
		(avgcpu, stdcpu) = job.get_agg_cpu()
		print job.id, job.numtasks, int( avgmem / (1024*1024)) , int( stdmem / (1024*1024)), job.get_agg_cpu()[0], job.get_duration()
	return jobs

	
def run_experiment(filename):

	print("start");


	numplayers = 50
	jobs = read_trace(filename, 7000)

	random_player_index = random.sample(range(len(jobs)), numplayers)	
	players = numpy.zeros((numplayers,2))
	for i in range(numplayers):
		#print i
		#print random_player_index
		j = random_player_index[i]
		players[i,0] = jobs[j-1].get_agg_mem()[0] / (1024 * 1024 * 1024 )
		players[i,1] = jobs[j-1].get_agg_cpu()[0]
		print "JOB : ",j-1, jobs[j-1].numtasks, players[i,0], players[i,1]
	
	machines = numpy.array([ [16 ,12.0], [ 8 ,12.0] ])
	machinesPerType = [ numpy.array([200, 200])]
	

	#exit()
	numberOfPlayers = numplayers
	numberOfExamples = 1
	numberOfMachineTypes = 2


	numberOfResources = 2
	machineAccuracy = 1
	machineLow = 200
	machineHigh = 300
	playerAccuracy = 10
	playerHigh = 20
	playerLow = 10

	clusterNumberLow = 1
	clusterNumberHigh = 2


	c = 0

	Utilities = numpy.zeros((numberOfExamples,6,numberOfPlayers))
	Utilizations = numpy.zeros((numberOfExamples,6,numberOfResources))

	while c < numberOfExamples :

			print "Experiment " + `c`

			#machines = floor( machineAccuracy*(machineLow + (machineHigh - machineLow)*random.random((numberOfMachineTypes,numberOfResources)) ))/machineAccuracy
			#players = floor( playerAccuracy*(playerLow + (playerHigh - playerLow)*random.random((numberOfPlayers,numberOfResources)) ))/playerAccuracy
			#machinesPerType = [random.randint(clusterNumberLow,clusterNumberHigh,numberOfMachineTypes)]

			
			#players = numpy.array( [ [1.0, 3.0], [2.0 , 1.0 ] ] )
			#machines = numpy.array([ [4.0,6.0], [4.0,4.0] ])
			#machinesPerType = [ numpy.array([100, 200])]

			print `machinesPerType`
			print `players`
			print `machines`
			#exit()
			sa = functions.computeStandAlones(machines, players)
			sa =  sa * numpy.transpose(machinesPerType)
			sa = numpy.sum(sa, axis=0)

			#print sa

			#[firstFitAllocation,randomFitAllocation,bestFitAllocation] = BigMachineLP.bigMachineDRF(machines,players,machinesPerType)
			#cdrfSupport = CDRF_support.cdrfSupportsFit(machines,players,machinesPerType,c)
			#bestFitAllocation = BigMachineLP.bigMachineDRF(machines,players,machinesPerType,0)


			'''
			iterativeDrfAllocationStatic_DomShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 0 ,0 )
			iterativeDrfAllocationStatic_SaShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 1 ,0 )
			'''


			#iterativeDrfAllocationDynamic_DomShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 0 ,1 )
			#iterativeDrfAllocationDynamic_SaShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 1 ,1 )
		

			#calculate utilities
			#utilitiesFF = findUtilities(firstFitAllocation,sa,numberOfPlayers,machinesPerType)
			#utilitiesRF = findUtilities(randomFitAllocation,sa,numberOfPlayers,machinesPerType)
			#utilitiesBF = measurePerformance.findUtilities(bestFitAllocation,numberOfPlayers,machinesPerType,sa)
			#utilitiesCDRFSupport = measurePerformance.findUtilities(cdrfSupport,numberOfPlayers,machinesPerType)#,sa)
			
			'''
			utilitiesItDRFStatic_DomShare = measurePerformance.findUtilities(iterativeDrfAllocationStatic_DomShare,numberOfPlayers,machinesPerType,sa)
			utilitiesItDRFStatic_SaShare = measurePerformance.findUtilities(iterativeDrfAllocationStatic_SaShare,numberOfPlayers,machinesPerType,sa)
			'''



			#utilitiesItDRFDynamic_DomShare = measurePerformance.findUtilities(iterativeDrfAllocationDynamic_DomShare,numberOfPlayers,machinesPerType,sa)
			#utilitiesItDRFDynamic_SaShare = measurePerformance.findUtilities(iterativeDrfAllocationDynamic_SaShare,numberOfPlayers,machinesPerType,sa)



			#utilitiesCDRF = measurePerformance.findUtilities(cdrfAllocation,numberOfPlayers,machinesPerType,sa)
			#utilitiesCDRF1 = measurePerformance.findUtilities(cdrfAllocation1,numberOfPlayers,machinesPerType,sa)



			#print "Utilities FF: " + `utilitiesFF`
			#print "Utilities RF: " + `utilitiesRF`
			#print "Utility BF: " + `utilitiesBF`
			#print "Utility CDRF Support: " + `utilitiesCDRFSupport`
			'''
			print
			print "Utility IterativeDRFStatic_DomShare: " + `utilitiesItDRFStatic_DomShare`
			'''
			
			#print "Utility Iterative DRF: " + `utilitiesItDRF`
			#print "Utility CDRF: " + `utilitiesCDRF`
			#print "Utility CDRF1: " + `utilitiesCDRF1`


			print
			#print "Min Utility BF: " + `min(utilitiesBF)`
			#print "Min Utility CDRF Support: " + `min(utilitiesCDRFSupport)`

			'''
			print "Min Utility IterativeDRFStatic_DomShare: " + `min(utilitiesItDRFStatic_DomShare)`
			print "Min Utility utilitiesItDRFStatic_SaShare: " + `min(utilitiesItDRFStatic_SaShare)`
			'''

			#print "Min Utility utilitiesItDRFDynamic_DomShare: " + `min(utilitiesItDRFDynamic_DomShare)`
			#print "Min Utility utilitiesItDRFDynamic_SaShare: " + `min(utilitiesItDRFDynamic_SaShare)`
			


			#utilizationBF = measurePerformance.findUtilization(bestFitAllocation,players,machinesPerType,machines)
			#utilizationCDRFSupport = measurePerformance.findUtilization(cdrfSupport,players,machinesPerType,machines)
			'''
			utilizationItDRFStatic_DomShare = measurePerformance.findUtilization(iterativeDrfAllocationStatic_DomShare,players,machinesPerType,machines)
			utilizationItDRFStatic_SaShare = measurePerformance.findUtilization(iterativeDrfAllocationStatic_SaShare,players,machinesPerType,machines)
			'''
			#utilizationItDRFDynamic_DomShare = measurePerformance.findUtilization(iterativeDrfAllocationDynamic_DomShare,players,machinesPerType,machines)
			#utilizationItDRFDynamic_SaShare = measurePerformance.findUtilization(iterativeDrfAllocationDynamic_SaShare,players,machinesPerType,machines)
	
			cdrfAllocation = CDRF.computeCDRF(machines,players,machinesPerType,c,0)
			cdrfAllocation1 = CDRF.computeCDRF(machines,players,machinesPerType,c,1)


			utilizationCDRF = measurePerformance.findUtilization(cdrfAllocation,players,machinesPerType,machines)
			utilizationCDRF1 = measurePerformance.findUtilization(cdrfAllocation1,players,machinesPerType,machines)

			print
			print "Utilization CDRF: " + `utilizationCDRF`
			print "Utilization CDRF1: " + `utilizationCDRF1`

			#print "Min Utility CDRF: " + `min(utilitiesCDRF)`
			#print "Min Utility CDRF1: " + `min(utilitiesCDRF1)`


			iterativeDrfAllocationStatic_DomShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 0 ,0 )
			iterativeDrfAllocationStatic_SaShare = IterativeDRF.iterativeDRF(machines,players,machinesPerType , sa , 1 ,0 )
			utilitiesItDRFStatic_DomShare = measurePerformance.findUtilities(iterativeDrfAllocationStatic_DomShare,numberOfPlayers,machinesPerType,sa)
			utilitiesItDRFStatic_SaShare = measurePerformance.findUtilities(iterativeDrfAllocationStatic_SaShare,numberOfPlayers,machinesPerType,sa)
			utilizationItDRFStatic_DomShare = measurePerformance.findUtilization(iterativeDrfAllocationStatic_DomShare,players,machinesPerType,machines)
			utilizationItDRFStatic_SaShare = measurePerformance.findUtilization(iterativeDrfAllocationStatic_SaShare,players,machinesPerType,machines)
			#print "Min Utility IterativeDRFStatic_DomShare: " + `min(utilitiesItDRFStatic_DomShare)`
			#print "Min Utility utilitiesItDRFStatic_SaShare: " + `min(utilitiesItDRFStatic_SaShare)`

			print
			print "Utilization Iterative DRFStatic_DomShare: " + `utilizationItDRFStatic_DomShare`
			print "Utilization Iterative DRFStatic_SaShare: " + `utilizationItDRFStatic_SaShare`

			#print "Utilization BF: " + `utilizationBF`
			#print "Utilization CDRF Support: " + `utilizationCDRFSupport`
			#print "Utilization Iterative DRFStatic_DomShare: " + `utilizationItDRFStatic_DomShare`
			#print "Utilization Iterative DRFStatic_SaShare: " + `utilizationItDRFStatic_SaShare`
			#print "Utilization Iterative DRFDynamic_DomShare: " + `utilizationItDRFDynamic_DomShare`
			#print "Utilization Iterative DRFDynamic_SaShare: " + `utilizationItDRFDynamic_SaShare`

			#Utilities[c][0] = utilitiesItDRFStatic_DomShare
			#Utilities[c][1] = utilitiesItDRFStatic_SaShare
			#Utilities[c][2] = utilitiesItDRFDynamic_DomShare
			#Utilities[c][3] = utilitiesItDRFDynamic_SaShare
			#Utilities[c][4] = utilitiesCDRF
			#Utilities[c][5] = utilitiesCDRF1

			#Utilizations[c][0] = utilizationItDRFStatic_DomShare
			#Utilizations[c][1] = utilizationItDRFStatic_SaShare
			#Utilizations[c][2] = utilizationItDRFDynamic_DomShare
			#Utilizations[c][3] = utilizationItDRFDynamic_SaShare
			Utilizations[c][4] = utilizationCDRF
			Utilizations[c][5] = utilizationCDRF1


			c = c+1


if __name__ == "__main__":
	run_experiment("sorted_jobs-exp-small")	
