import os
import sys

dirname = "results"
if len(sys.argv) > 1:
	dirname = sys.argv[1].strip()

cdrf_drf_comparison = []
drf_bf_ff_comparison = []

count = 0
for filename in os.listdir(dirname):
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

		print "Num Elephants", num_elephants, " > ", num_tasks, "machine types : ",machine_types
		print lines[ num_lines - 4].strip()
		print lines[ num_lines - 3].strip()
		print lines[num_lines - 2].strip()
		print lines[num_lines - 1].strip()

		print "CDRF vs DRF_BF : %.2f,%.2f " % ( (util_cdrf_1_mem - util_drf_ff_mem) , (util_cdrf_1_cpu - util_drf_ff_cpu) )
		print "DRF_BF vs DRF_FF : %.2f,%.2f" % ( (util_drf_bf_mem - util_drf_ff_mem ), (util_drf_bf_cpu - util_drf_ff_cpu ) )

		if util_cdrf_1_mem - util_drf_ff_mem > 0.01 or util_cdrf_1_cpu - util_drf_ff_cpu > 0.01 :
			print "CDRF wins over DRF_FF - ", "DIFF : (%.2f, %.2f)" % (util_cdrf_1_mem - util_drf_ff_mem, util_cdrf_1_cpu - util_drf_ff_cpu)   ," DRF_FF (%.2f, %.2f) vs CDRF (%.2f, %.2f)" % ( util_drf_ff_mem, util_drf_ff_cpu, util_cdrf_1_mem, util_cdrf_1_cpu ), filename
		if util_drf_bf_mem - util_drf_ff_mem > 0.01 or util_drf_bf_cpu - util_drf_ff_cpu > 0.01 :
			print "DRF_BF wins over DRF_FF", "DIFF : (%.2f, %.2f)" % (util_drf_bf_mem - util_drf_ff_mem, util_drf_bf_cpu - util_drf_ff_cpu)   , " DRF_FF (%.2f, %.2f) vs CDRF (%.2f, %.2f)" % ( util_drf_ff_mem, util_drf_ff_cpu, util_drf_bf_mem, util_drf_bf_cpu ), filename
		if util_cdrf_1_mem - util_drf_bf_mem > 0.01 or util_cdrf_1_cpu - util_drf_bf_cpu > 0.01 :
			print "CDRF wins over DRF_BF - ", "DIFF : (%.2f, %.2f)" % (util_cdrf_1_mem - util_drf_bf_mem, util_cdrf_1_cpu - util_drf_bf_cpu)   ," DRF_BF (%.2f, %.2f) vs CDRF (%.2f, %.2f)" % ( util_drf_bf_mem, util_drf_bf_cpu, util_cdrf_1_mem, util_cdrf_1_cpu ), filename


		print "==============="
		print "\n\n"

	except :
		continue
print "Total number of output files : ",count
