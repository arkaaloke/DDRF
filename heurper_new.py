import numpy
import functions
import CDRF
import measurePerformance
import IterativeDRF
import cPickle as pickle
import math
import random
import sys, traceback


class Task:
    def __init__(self, mem, cpu, duration):
        self.mem = mem
        self.cpu = cpu
        self.duration = duration


class Job:
    def __init__(self, id, start, numtasks):
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
            cpu = float(parts[2]) / float(parts[1])
            duration = float(parts[1]) / 1000

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
    jobs = []  # using systems terminology
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

        if numtasks < threshold:
            lineno += (numtasks + 1)
            continue
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
        print job.id, job.numtasks, int(avgmem / (1024 * 1024)), int(stdmem / (1024 * 1024)), job.get_agg_cpu()[
            0], job.get_duration()
    return jobs


def applyThreshold(allJobs, threshold ):
    newJobs = []
    for job in allJobs:
        if job.get_num_tasks() > threshold:
            newJobs.append(job)

    return newJobs

def run_experiment(jobs, threshold, numplayers, machines, machinesPerType, outputfilename):
    print("start");
    outputfile = open(outputfilename, "w")
    #jobs = applyThreshold(allJobs, threshold )
    players = numpy.zeros((numplayers, 2))
    #if numplayers > len(jobs):
    #    print >> sys.stderr, "cannot find", jobs, " with more than ",threshold, "tasks"
    #random_player_index = random.sample(range(len(jobs)), numplayers)
    requests = numpy.zeros(numplayers)

    for i in range(numplayers):
        #j = random_player_index[i]
        players[i, 0] = jobs[i].get_agg_mem()[0] / (1024 * 1024 * 1024)
        players[i, 1] = jobs[i].get_agg_cpu()[0]
        requests[i] = jobs[i].numtasks

    #machines = numpy.array([[16.0, 12.0], [6.0, 12.0], [14.0, 4.0]])
    #machinesPerType = [numpy.array([200, 200, 100])]
    for i in range(len(machines)):
        dominantResources = (numpy.divide(players, machines[i])).argmax(axis=1)
        print >>outputfile, "DR machine :",i, ": ", sum(dominantResources)  #, dominantResources
    
    #exit()
    numberOfPlayers = numplayers
    numberOfExamples = 1
    numberOfMachineTypes = len(machines)

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

    Utilities = numpy.zeros((numberOfExamples, 6, numberOfPlayers))
    Utilizations = numpy.zeros((numberOfExamples, 6, numberOfResources))

    while c < numberOfExamples:
        print "Experiment " + `c`

        #machines = floor( machineAccuracy*(machineLow + (machineHigh - machineLow)*random.random((numberOfMachineTypes,numberOfResources)) ))/machineAccuracy
        #players = floor( playerAccuracy*(playerLow + (playerHigh - playerLow)*random.random((numberOfPlayers,numberOfResources)) ))/playerAccuracy
        #machinesPerType = [random.randint(clusterNumberLow,clusterNumberHigh,numberOfMachineTypes)]
        numpy.set_printoptions(threshold=numpy.nan)
        #print numpy.floor(100*players)/100

        sa = functions.computeStandAlones(machines, players)
        sa = sa * numpy.transpose(machinesPerType)
        sa = numpy.sum(sa, axis=0)
        players = numpy.floor(100 * players) / 100

        print >> outputfile, "machines : ",machines
        print >>outputfile, "machinesPerType : ", machinesPerType
        iterativeDrfAllocationStatic_BF = IterativeDRF.iterativeDRF(machines, players, machinesPerType, sa, 0, 0)
        iterativeDrfAllocationStatic_FF = IterativeDRF.iterativeDRF(machines, players, machinesPerType, sa, 1, 0)
        #sys.exit()
        cdrfAllocation = CDRF.computeCDRF(machines, players, machinesPerType, c, 0)
        cdrfAllocation1 = CDRF.computeCDRF(machines, players, machinesPerType, c, 1)



        #calculate utilities
        utilitiesItDRFStatic_BF = measurePerformance.findUtilities(iterativeDrfAllocationStatic_BF, numberOfPlayers,
                                                                   machinesPerType)  #,sa)
        utilitiesItDRFStatic_FF = measurePerformance.findUtilities(iterativeDrfAllocationStatic_FF, numberOfPlayers,
                                                                   machinesPerType)  #,sa)
        utilitiesCDRF = measurePerformance.findUtilities(cdrfAllocation, numberOfPlayers, machinesPerType)  #,sa)
        utilitiesCDRF1 = measurePerformance.findUtilities(cdrfAllocation1, numberOfPlayers, machinesPerType)  #,sa)

        for i in range(numberOfPlayers):
            print >>outputfile, "Player ", i, "with demand", players[i], "asked for", requests[i], "tasks"
            print >>outputfile, "BF: ", utilitiesItDRFStatic_BF[i], "FF: ",utilitiesItDRFStatic_FF[i], "CDRF:",utilitiesCDRF[i], "CDRF1:", utilitiesCDRF1[i]


        utilizationItDRFStatic_BF = measurePerformance.findUtilization(iterativeDrfAllocationStatic_BF, players,machinesPerType, machines)
        utilizationItDRFStatic_FF = measurePerformance.findUtilization(iterativeDrfAllocationStatic_FF, players,machinesPerType, machines)
        utilizationCDRF = measurePerformance.findUtilization(cdrfAllocation, players, machinesPerType, machines)
        utilizationCDRF1 = measurePerformance.findUtilization(cdrfAllocation1, players, machinesPerType, machines)

        print
        print >>outputfile,  "Utilization Iterative DRFStatic_FF: ", utilizationItDRFStatic_FF[0], ",", utilizationItDRFStatic_FF[1]
        print >>outputfile, "Utilization Iterative DRFStatic_BF: ", utilizationItDRFStatic_BF[0], ",", utilizationItDRFStatic_BF[1]
        print >>outputfile, "Utilization CDRF: ", utilizationCDRF[0] , ",", utilizationCDRF[1]
        print >>outputfile, "Utilization CDRF1: ", utilizationCDRF1[0], ",", utilizationCDRF1[1]

        c = c + 1


if __name__ == "__main__":
    run_experiment("sorted_jobs-exp-small")
