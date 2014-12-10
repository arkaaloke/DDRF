import os
import sys
import numpy
dirname = "results3"
if len(sys.argv) > 1:
	dirname = sys.argv[1].strip()

cdrf_drf_comparison = []
drf_bf_ff_comparison = []

count = 0
runs = {}
for filename in os.listdir(dirname):
	iteration = filename.split("-")[-1].strip()
	runname = filename.split("-" + iteration)[0].strip()
	if runname not in runs :
		runs[runname] = {}
		runs[runname]["cdrf"] = []
		runs[runname]["drf_ff"] = []
		runs[runname]["drf_bf"] = []

	count += 1
	filename = filename.strip()
	num_elephants = filename.split("-")[0].strip()
	num_tasks = filename.split("-")[1].strip()
	machine_types = filename.split("-" + num_tasks + "-")[-1].strip()

	try :

		lines = open(dirname  + "/" + filename).readlines()
		num_lines = len(lines)

		util_drf_ff_cpu = float(lines[ num_lines - 4 ].split(":")[-1].split(",")[-1].strip())
		util_drf_ff_mem = float(lines[ num_lines - 4 ].split(":")[-1].split(",")[0].strip())

		util_drf_bf_cpu = float(lines[ num_lines - 3 ].split(":")[-1].split(",")[-1].strip())
		util_drf_bf_mem = float(lines[ num_lines - 3 ].split(":")[-1].split(",")[0].strip())

		util_cdrf_1_cpu = float(lines[ num_lines - 2 ].split(":")[-1].split(",")[-1].strip())
		util_cdrf_1_mem = float(lines[ num_lines - 2 ].split(":")[-1].split(",")[0].strip())

		util_cdrf_2_cpu = float(lines[ num_lines - 1 ].split(":")[-1].split(",")[-1].strip())
		util_cdrf_2_mem = float(lines[ num_lines - 1 ].split(":")[-1].split(",")[0].strip())

		#print "Num Elephants", num_elephants, " > ", num_tasks, "machine types : ",machine_types
		#print lines[ num_lines - 4].strip()
		#print lines[ num_lines - 3].strip()
		#print lines[num_lines - 2].strip()
		#print lines[num_lines - 1].strip()

		#print "CDRF vs DRF_BF : %.2f,%.2f " % ( (util_cdrf_1_mem - util_drf_ff_mem) , (util_cdrf_1_cpu - util_drf_ff_cpu) )
		#print "DRF_BF vs DRF_FF : %.2f,%.2f" % ( (util_drf_bf_mem - util_drf_ff_mem ), (util_drf_bf_cpu - util_drf_ff_cpu ) )

		runs[runname]["cdrf"].append((util_cdrf_1_mem, util_cdrf_1_cpu))
		runs[runname]["drf_ff"].append((util_drf_ff_mem, util_drf_ff_cpu))
		runs[runname]["drf_bf"].append((util_drf_bf_mem, util_drf_bf_cpu))
	except :
		continue


summary_cdrf_drf_ff = {}
summary_cdrf_drf_bf = {}
summary_drf_bf_drf_ff = {}

for run in runs:
    diff = []
    for i in range(min(len(runs[run]["cdrf"]), len(runs[run]["drf_ff"]))):
        diff.append( ( runs[run]["cdrf"][i][0] - runs[run]["drf_ff"][i][0], runs[run]["cdrf"][i][1] - runs[run]["drf_ff"][i][1]) )

    mem_diff = [ m for (m,c) in diff ]
    mem_diff_mean = numpy.mean(numpy.array( mem_diff))
    mem_diff_std = numpy.std( numpy.array( mem_diff))


    cpu_diff = [ c for (m,c) in diff ]
    cpu_diff_mean = numpy.mean(numpy.array( cpu_diff))
    cpu_diff_std = numpy.std( numpy.array( cpu_diff))
    summary_cdrf_drf_ff[run] =  (mem_diff_mean, mem_diff_std, cpu_diff_mean, cpu_diff_std)

print "\n\nSUMMARY\n\n"
for run in sorted(summary_cdrf_drf_ff, key=lambda k:summary_cdrf_drf_ff[k][0] + summary_cdrf_drf_ff[k][2], reverse=True):
    #print run
    #print summary_cdrf_drf_ff[run]
    print run
    cdrf_mem = numpy.mean( [ mem for (mem,cpu) in runs[run]["cdrf"] ] )
    cdrf_cpu = numpy.mean( [cpu for (mem, cpu) in runs[run]["cdrf"] ])
    drf_ff_mem = numpy.mean( [ mem for (mem,cpu) in runs[run]["drf_ff"] ])
    drf_ff_cpu = numpy.mean( [cpu for (mem, cpu) in runs[run]["drf_ff"] ])
    drf_bf_mem = numpy.mean( [ mem for (mem,cpu) in runs[run]["drf_bf"] ])
    drf_bf_cpu = numpy.mean( [cpu for (mem, cpu) in runs[run]["drf_bf"] ])

    print "CDRF : (%.2f,%.2f) DRF_BF : (%.2f,%.2f) DRF_FF : (%.2f,%.2f)" % (cdrf_mem, cdrf_cpu, drf_bf_mem, drf_bf_cpu, drf_ff_mem, drf_ff_cpu)
    
    #print run," DIFF mean : (%.2f, %.2f)" %  (summary_cdrf_drf_ff[run][0], summary_cdrf_drf_ff[run][2]), ",std : (%.2f, %.2f)" %  (summary_cdrf_drf_ff[run][1], summary_cdrf_drf_ff[run][3]) 
    #for i in range( min(len(runs[run]["cdrf"]) , len(runs[run]["drf_ff"]) ) ):
    #    print " CDRF:(%.2f, %.2f)" % ( runs[run]["cdrf"][i][0], runs[run]["cdrf"][i][1] ) +  ",DRF_FF:(%.2f, %.2f)" % ( runs[run]["drf_ff"][i][0], runs[run]["drf_ff"][i][1] ) + ", ",
    print "\n\n==============="
    print "\n\n"
    
print "Total number of output files : ",count
