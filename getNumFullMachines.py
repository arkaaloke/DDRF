import os
import sys
import numpy
import re

def readfile(filename, numMachines, simDuration):
	numMachines = 399
	machines = {}
	for i in range(numMachines):
		machines[i] = numpy.zeros(simDuration + 1)

	simTime = 0

	machine_state = { "total full" : 1 , "total free" : 2 , "elephant full" : 3 , "elephant free" : 4, "mice full" : 5 , "mice free" : 6 }

	for line in open(filename).readlines():
		if "Simulation Time :" in line:
			simTime = int(line.split(",")[0].strip().split(":")[-1].strip())					
			print "Done simTime : ", simTime
			continue

		if "Machine :" in line:
			machineId = int(re.split('\ +',line.strip())[2].strip())
			status = re.split('\ +',line.strip())[3].strip() + " " + re.split('\ +',line.strip())[4].strip()
			if machines[machineId][simTime] == 0:
				machines[machineId][simTime] = machine_state[status]
			else:
				if "free" in status and machines[machineId][simTime] % 2 == 1:
					continue
				machines[machineId][simTime] = machine_state[status]

	for i in range(numMachines):
		j = 0
		prevState = 0
		while j < simDuration - 1:
			try:
				if machines[i][j+1] == 0:
					machines[i][j+1] = machines[i][j]
					j += 1
					continue
				else:
					prevState = machines[i][j]
					j += 1
					continue
			except:
				print i, j 
				raise
	print "Time," ,
	for m in range( numMachines ):
		print m , "," ,
	print 

	numFreeMachines = {}
	for t in range( simDuration ):
		print t , ", " ,

		num_free = []
		for m in range( numMachines ):
			print machines[m][t] , "," ,
			if machines[m][t] % 2 == 0:
				num_free.append((m, machines[m][t]))
		numFreeMachines[t] = num_free

		print

	print "Time, Number of free machines"
	for t in range(simDuration):
		print t, ",", len(numFreeMachines[t]) , "," , numFreeMachines[t]

if __name__ == "__main__":
	readfile( sys.argv[1].strip() , 399, 40000) 
