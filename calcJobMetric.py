import os
import sys
import numpy
from datetime import datetime

f = open("sorted_jobs-exp")

maxMem = { 10: [] , 50:[], 100: [], 500:[], 1000:[], 5000:[], 10000:[] , 1000000:[] }
maxCpu = { 10: [] , 50:[], 100: [], 500:[], 1000:[], 5000:[], 10000:[] , 1000000:[] }

count = 0
lineCount = 0
startTime = None
finishTime = None


while True:
	try :
		lineCount += 1
		line = f.readline().strip()
		if not line:
			break
		count += 1
		if count % 1000 == 0:
			print "Done ", count

		parts = line.split(":")
		if parts[0].strip() != "j":
			print " Something wrong with the input file"
			exit()
		starttime = int(parts[1].strip())
		if startTime == None:
			startTime = datetime.fromtimestamp(starttime)

		finishTime = datetime.fromtimestamp(starttime)

		numtasks = int(parts[2].strip())


		tasks = []
		memArray = []
		cpuArray = []
		for i in range(numtasks):
			line = f.readline() 
			lineCount += 1
			parts = line.strip().split(":")
			if parts[0].strip() != "t":
				exit()
			mem = int(parts[3].strip())
			cpu = float(parts[2]) / float(parts[1])
			duration = int(float(parts[1]) / 1000)


			memArray.append(mem)
			cpuArray.append(cpu)

		avg_mem = numpy.mean(memArray) / (1024 * 1024 *1024)
		avg_cpu = numpy.mean(cpuArray)

		for key in sorted(maxMem):
			if numtasks <= key:
				maxMem[key].append(avg_mem)
				break

		for key in sorted(maxCpu):
			if numtasks <= key:
				maxCpu[key].append(avg_cpu)
				break

	except :
		print "Unexpected error:", sys.exc_info()[0]
		#raise
		print "Line count : ", lineCount
		print "Number of jobs : ", count
		
		break

print "Trace start time : ", startTime
print "Trace Finish time : ", finishTime
print "Duration of trace : ", finishTime - startTime

for key in sorted(maxMem):
	print key, "tasks. . Percent Jobs : ", len(maxMem[key]) * 100.0 / count 
	print "MAX : mem : (%.1f) cpu : (%.1f) " %( numpy.max(maxMem[key]) , numpy.max(maxCpu[key]) ) ,
	print "MIN : mem : (%.1f) cpu : (%.1f) " %( numpy.min(maxMem[key]) , numpy.min(maxCpu[key]) ) ,
	print "AVG : mem : (%.1f) cpu : (%.1f) " %( numpy.mean(maxMem[key]) , numpy.mean(maxCpu[key]) ) ,
	print "STD : mem : (%.1f) cpu : (%.1f) " %( numpy.std(maxMem[key]) , numpy.std(maxCpu[key]) ) ,
	print 


