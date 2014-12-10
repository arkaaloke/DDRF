import os
import sys
import numpy
import random

jobs = None

class Job :
    def __init__(self, jobid, starttime, numtasks, lines):
        self.lines = lines
        self.num_tasks = numtasks

    def get_num_tasks(self):
        return self.num_tasks

    def get_lines(self):
        return self.lines

def read_trace(filename):
    global jobs
    jobs = []  # using systems terminology
    f = open(filename)
    print "Reading file "
    lines = f.readlines()
    print "Finished reading file"
    numlines = len(lines)
    jobid = 1
    lineno = 0
    while lineno < numlines:
        if jobid % 1000 == 0:
            print "Read job : ",jobid
        parts = lines[lineno].strip().split(":")
        if parts[0].strip() != "j":
            print " Something wrong with the input file"
            exit()
        starttime = int(parts[1].strip())
        numtasks = int(parts[2].strip())

        j = Job(jobid, starttime, numtasks, lines[lineno : lineno + numtasks + 1])
        jobs.append(j)
        jobid += 1
        lineno += (numtasks + 1)


    print "Finished reading jobs "
    f.close()

def applyThreshold(threshold ):
    global jobs

    newJobs = []
    for job in jobs:
        if job.get_num_tasks() > threshold:
            newJobs.append(job)

    return newJobs


def main():
    global jobs
    dirname = "jobfiles"
    print "Parsing jobs"
    read_trace(sys.argv[1].strip())
    print "Finished parsing jobs"
    numElephants = [  20 , 40 , 60 , 80 , 100 ]  
    elephantNumTasks = [ 10 , 50 , 100 , 500 , 1000 , 5000 ]


    for ele_num_tasks in elephantNumTasks:
        new_jobs = applyThreshold (ele_num_tasks)
        for num_ele in numElephants:
            if num_ele > len(new_jobs):
                print "not enough jobs in trace (%d) to get %d jobs" % (len(new_jobs) , num_ele)
            for r in range(20):
                print "Doing Threshold %d NumberOfJobs %d iteration %d" % (ele_num_tasks,num_ele, r)
                random_player_index = random.sample(range(len(new_jobs)), num_ele)
                random_job_list = []
                for i in range(num_ele):
                    j = random_player_index[i]
                    random_job_list.append(new_jobs[j - 1])
                filename = str(ele_num_tasks) + "-" + str(num_ele) + "-" + str(r)
                f = open(dirname + "/" + filename, "w")
                for j in random_job_list:
                    lines = j.get_lines()
                    for line in lines:
                        f.write(line.strip() + "\n")

                f.close()


if __name__ == "__main__":
    main() 
