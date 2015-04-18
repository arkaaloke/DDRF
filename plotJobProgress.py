import os
import sys
import matplotlib.pyplot as plt
 
jobs = {}

jobs["ddrf"] = {}
jobs["drf"] = {}

'''
USAGE : python plotJobProgress.py <DDRF file> <DRF file>
'''


lines = open(sys.argv[1]).readlines()
print "Read DDRF file"
for line in lines:
	line = line.split(":")[-1].strip()
	time = int(line.split("(")[0].strip())
	#if time > 10000:
	#	break
	if "(" not in line:
		continue
	remaining_line = line[ line.index("("): ].strip()
	parts = remaining_line.split(") , (")
	print "Doing time : ", time
	for part in parts:
		part = part.split(")")[0].strip()
		part = part.split("(")[-1].strip()
		jobid = part.strip().split(",")[0].strip()
		if jobid not in jobs["ddrf"]:
			jobs["ddrf"][jobid] = {}
		numTasks = int(part.strip().split(",")[2].strip())
		share = float(part.strip().split(",")[1].strip())
		#print "jobid : ", jobid, "numtasks : ", numTasks, "share : ", share, 
		jobs["ddrf"][jobid][time]  = ( numTasks, share )

	print


lines = open(sys.argv[2]).readlines()
for line in lines:
	line = line.split(":")[-1].strip()
	time = int(line.split("(")[0].strip())
	#if time > 10000:
	#	continue
	if "(" not in line:
		continue
	print "Doing time (DRF) : ", time
	#print line
	remaining_line = line[ line.index("("): ].strip()
	parts = remaining_line.split(") , (")
	for part in parts:
		part = part.split(")")[0].strip()
		part = part.split("(")[-1].strip()

		jobid = part.strip().split(",")[0].strip()
		if jobid not in jobs["drf"]:
			jobs["drf"][jobid] = {}
		numTasks = int(part.strip().split(",")[2].strip())
		share = float(part.strip().split(",")[1].strip())

		jobs["drf"][jobid][time]  = ( numTasks, share )

for jobid in jobs["drf"]:
	# get ddrf data
	if jobid not in jobs["ddrf"]:
		continue
	max_share = None
	max_numtasks = None

	t_ddrf = []
	share_ddrf = []
	numTasks_ddrf = []
	for times in sorted(jobs["ddrf"][jobid]):
		t_ddrf.append(times)
		numTasks_ddrf.append( jobs["ddrf"][jobid][times][0] )
		share_ddrf.append( jobs["ddrf"][jobid][times][1] )

	t_drf = []
	share_drf = []
	numTasks_drf = []
	for times in sorted(jobs["drf"][jobid]):
		t_drf.append(times)
		numTasks_drf.append( jobs["drf"][jobid][times][0] )
		share_drf.append( jobs["drf"][jobid][times][1] )

	#print jobid, "DRF"
	#print t_drf
	#print share_drf
	#print numTasks_drf

	#print jobid, "DDRF"
	#print t_ddrf
	#print share_ddrf
	#print numTasks_ddrf

	max_share = max( max(share_drf), max(share_ddrf) )
	max_numtasks = max( max(numTasks_drf) , max(numTasks_ddrf) )


	fig, ax1 = plt.subplots()
	ax2 = ax1.twinx()

	ax1.plot( t_ddrf, share_ddrf, 'g-', label="share-DDRF" )
	ax1.plot( t_drf, share_drf , 'b-', label="share-DRF" )
	ax2.plot( t_ddrf, numTasks_ddrf, 'c-', label="numTasks-DDRF")
	ax2.plot( t_drf, numTasks_drf, 'k-', label="numTasks-DRF")

	ax1.set_xlabel(' Time ')
	ax1.set_ylabel('Share')
	ax2.set_ylabel('NumTasks')
	ax1.set_ylim(0, max_share + 0.2 )
	ax2.set_ylim(0, max_numtasks + 20 )
	plt.legend()
	ax2.legend(loc=0)
	plt.savefig( jobid + ".png", dpi=200)

	plt.close() 

 
