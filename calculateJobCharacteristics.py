import os
import sys
from Job import *
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
 
    inputdir = sys.argv[1].strip()

    jobs = None 
    #jobs = read_trace(sys.argv[1].strip(), 50000)
    numElephants = [  20 , 40 , 60 , 80 , 100 ]  
    elephantNumTasks = [ 10 , 50 , 100 , 500 , 1000 , 5000 ]
    for numele in numElephants:
        print >> sys.stderr, "Doing numElephants : ", numele  
        for eletasks in elephantNumTasks: 
            all_mem = []
            all_cpu = []
            print "========= EXP ========="
            print "nuemele : ", numele
            print "eletasks : ", eletasks
            for i in range(20):
                mem = []
                cpu = []
                jobs = read_trace(inputdir + "/" + str(eletasks) + "-" + str(numele) + "-" + str(i) , 500000)
                for job in jobs:
                    mem.append(job.get_agg_mem()[0] / (1024*1024 * 1024))
                    cpu.append( job.get_agg_cpu()[0] )
                    

                avg_mem = numpy.mean(mem)
                avg_cpu = numpy.mean(cpu)
                std_mem = numpy.std(mem)
                std_cpu = numpy.std(cpu)
                print "Run %d - mean : (%.2f, %.2f) std : (%.2f, %.2f) " % (i, avg_mem, avg_cpu, std_mem, std_cpu)
                all_mem.append(avg_mem)
                all_cpu.append(avg_cpu)

            print "Overall - mean : (%.2f, %.2f) std : (%.2f, %.2f) " % (numpy.mean(all_mem) , numpy.mean(all_cpu), numpy.std(all_mem), numpy.std(all_cpu))
            print "=======================\n\n\n"
    
             
if __name__ == "__main__":
    main()

