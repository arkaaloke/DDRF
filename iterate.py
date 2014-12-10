import os
import sys
from job import *
import numpy
import itertools
from heurper_new import *
import re

def read_trace(filename, numplayers):
    jobs = []  # using systems terminology
    f = open(filename)
    lines = f.readlines()
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
        lineno += (numtasks + 1)

        if jobid > numplayers:
            break

    print "Finished reading jobs "
    for job in jobs:
        (avgmem, stdmem) = job.get_agg_mem()
        (avgcpu, stdcpu) = job.get_agg_cpu()
        #print job.id, job.numtasks, int(avgmem / (1024 * 1024)), int(stdmem / (1024 * 1024)), job.get_agg_cpu()[0], job.get_duration()

    f.close()
    return jobs

def main():
    if len(sys.argv) < 2:
        print "Usage iterate.py <input_jobs_dir> <output_jobs_dir> <numpairs> <pair1> <pair2>"
  
    inputdir = sys.argv[1].strip()
    outputdir = sys.argv[2].strip()

    jobs = None 
    #jobs = read_trace(sys.argv[1].strip(), 50000)
    numElephants = [  20 , 40 , 60 , 80 , 100 ]  
    elephantNumTasks = [ 10 , 50 , 100 , 500 , 1000 , 5000 ]

    if len(sys.argv) > 3:
        numpairs = int(sys.argv[3].strip())
        numElephants = []
        elephantNumTasks = []
        for i in range(numpairs):
            numElephants.append( int(sys.argv[i+4].strip().split(":")[0].strip()) )
            elephantNumTasks.append( int(sys.argv[i+4].strip().split(":")[-1].strip()) )

    print numElephants
    print elephantNumTasks

    general = [ 30.0 , 30.0 ]
    mem_optimized = [ 30.0 , 4.0 ]
    cpu_optimized = [ 16 , 30.0 ]

    machine_types = [ general, mem_optimized, cpu_optimized ]
    machine_types_strings = [ "G" , "M", "C" ]
    totalMachines = 900

    subsets = []
    subsets_string = []

    for L in range(0, len(machine_types) + 1):
        for subset in itertools.combinations(machine_types, L):
            print numpy.array(subset) , len(numpy.array(subset))
            subsets.append(subset)
    
    for L in range(0, len(machine_types_strings) + 1):
        for subset in itertools.combinations(machine_types_strings, L):
            print subset
            subsets_string.append(subset)
    
    
    tot_machines = 900
    print >> sys.stderr, "Total machines :", tot_machines
    for numele in numElephants:
        print >> sys.stderr, "Doing numElephants : ", numele  
        for eletasks in elephantNumTasks: 
            print >>sys.stderr, "Num Tasks : ", eletasks
            for subset in subsets[7:]:
                print "machines-per-type : ",subset
                ss = subsets_string[ subsets.index(subset) ]
                subset_str = ""
                if len(ss) == 1:
                    subset_str = ss[0]
                else:
                    subset_str = ss[0]
                    for t in ss[1:]:
                        subset_str += "-" + t
                
                print subset, subset_str
                #continue
                num_machine_types = len(subset)
                num_machines_per_type = 900 / len(subset)
                machines_per_type = []
                for i in range(num_machine_types):
                       machines_per_type.append( num_machines_per_type )

                print >>sys.stderr, "running experiment with : ",numpy.array(subset) , [ numpy.array( machines_per_type ) ]

                print "========= EXP ========="
                print "nuemele : ", numele
                print "eletasks : ", eletasks
                print "machines : ", numpy.array(subset)
                print "machines per type : ",  [ numpy.array( machines_per_type ) ]
                print "======================="
                #print "machines per type : ", numpy.array(subsets[1])[0][0] 
                #print "machines : ",  numpy.array( [300] )[0]  
                for i in range(20):
                    jobs = read_trace(inputdir + "/" + str(eletasks) + "-" + str(numele) + "-" + str(i) , 500000)
                    run_experiment(jobs, eletasks , numele , numpy.array(subset) , [numpy.array( machines_per_type )] , outputdir + "/" + str(numele) + "-" + str(eletasks) + "-" + str(subset_str) + "-" + str(i) )

   
if __name__ == "__main__":
    main() 
