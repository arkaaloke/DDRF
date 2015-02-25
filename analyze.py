import os
import sys
import numpy

def getBucket(numTasks):
	global buckets
	
	for i in range(len(buckets)):
		if numTasks <= buckets[i]:
			return buckets[i]

def compare(drffilename, ddrffilename):
	global buckets

	print "\n\nComparing ", drffilename, " and ", ddrffilename, "\n"
	bucket_lines = open("buckets").readlines()
	drf_lines = open(drffilename).readlines()
	ddrf_lines = open(ddrffilename).readlines()

	drf_jobs = {}
	ddrf_jobs = {}

	for line in drf_lines[1:]:
		#print line
		jobid = line.split(",")[0].strip()
		drf_jobs[jobid] = {}
		drf_jobs[jobid]["start time"] = int(line.split(",")[4].strip())
		drf_jobs[jobid]["finish time"] = int(line.split(",")[6].strip())
		drf_jobs[jobid]["completion time"] = drf_jobs[jobid]["finish time"] - drf_jobs[jobid]["start time"]
		drf_jobs[jobid]["numtasks"] = int(line.split(",")[1].strip())

	for line in ddrf_lines[1:]:
		jobid = line.split(",")[0].strip()
		ddrf_jobs[jobid] = {}
		ddrf_jobs[jobid]["start time"] = int(line.split(",")[4].strip())
		ddrf_jobs[jobid]["finish time"] = int(line.split(",")[6].strip())
		ddrf_jobs[jobid]["completion time"] = ddrf_jobs[jobid]["finish time"] - ddrf_jobs[jobid]["start time"]
		ddrf_jobs[jobid]["numtasks"] = int(line.split(",")[1].strip())

	#print "Read output files"	   
	buckets = []

	#for line in bucket_lines:
	#	buckets.append(int(line.strip()))

	buckets.append( int(drffilename.strip().split(".")[-3].strip()) )
	buckets.append(1000000)

	#print buckets

	better_bucket_wise = {}
	num_jobs_completed_drf = {}
	num_jobs_completed_ddrf = {}

	for bucket in buckets:
		better_bucket_wise[bucket] = []
		num_jobs_completed_drf[bucket] = 0
		num_jobs_completed_ddrf[bucket] = 0

	for jobid in ddrf_jobs:
		if jobid not in drf_jobs:
			continue
		percent_better = ( drf_jobs[jobid]["completion time"] - ddrf_jobs[jobid]["completion time"]) * 100.0 / float(drf_jobs[jobid]["completion time"])
		bucket = getBucket(ddrf_jobs[jobid]["numtasks"])

		better_bucket_wise[bucket].append(percent_better)


	for jobid in ddrf_jobs:
		bucket = getBucket(ddrf_jobs[jobid]["numtasks"])
		num_jobs_completed_ddrf[bucket] += 1

	for jobid in drf_jobs:
		bucket = getBucket(drf_jobs[jobid]["numtasks"])
		num_jobs_completed_drf[bucket] += 1

	   
	print "Bucket , Mean % better (DDRF) , STD % better (DDRF ) , Num Jobs Completed (DRF) , Num Jobs Complted (DDRF)"
	for bucket in sorted(buckets):
		if len(better_bucket_wise[bucket]) == 0:
			continue
		print "<=" + str(bucket) , numpy.mean(better_bucket_wise[bucket]) , numpy.std(better_bucket_wise[bucket]) , num_jobs_completed_drf[bucket],   num_jobs_completed_ddrf[bucket]


if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Usage : python analyze.py <drffilename> <ddrffilename>"
		exit()
	compare(sys.argv[1], sys.argv[2])
