from numpy import *
import functions
import sys


def findScores(machines,players):
    n = shape(players)[0]
    m = shape(machines)[0]
    scores = zeros((n,m))

    for i in range(0,n):
        for j in range(0,m):
            #scores[i][j] = divide( dot(players[i],machines[j]) , linalg.norm(machines[j],2) * linalg.norm(players[i],2) )
            scores[i][j] = - sum(divide(players[i],machines[j]))

    return scores

def randomChoice(probs):
    r = random.random()
    s = 0
    i = 0
    for p in probs:
        s += p
        if s >= r:
            return i
        i += 1

    return 0

#finds fractional DRF the Parkes/Procaccia way
def findDRF(bigMachine,players):

    #find normalized demands
    D = divide( players , bigMachine)
    d = divide(D , transpose([D.max(axis=1)]) )
    x =  1 / (sum(d, axis = 0).max())

    allocation = (x*d) * bigMachine
    tasks =  divide( allocation, players)
    return [allocation,tasks[:,0]]

#fits players into machines using FirstFit
def FirstFit(machines,players,taskList):
    print "first fit"
    n = shape(players)[0]
    m = shape(machines)[0]
    allocation = zeros(n*m)
    start = 0

    #while someone still fits
    while (taskList > 0).any():
        #pick random user based on taskList size
        probs = taskList / sum(taskList)
        user = randomChoice(probs)

        if taskList[user] > 0:
            fits = False

            #pick the first machine that fits
            for j in range(start,m):
                test = allocation[ j*n : (j+1)*n ].copy()
                test[user] += 1
                if functions.isFeasible(machines[j],players, test ):
                    allocation[j*n+user] += 1
                    taskList[user] = taskList[user] - 1
                    fits = True
                    break

            if not fits:
                taskList[user] = 0

        if start < m:
            #update start
            update = True
            for i in range(0,n):
                test = allocation[ start*n : (start+1)*n ].copy()
                test[i] += 1
                if functions.isFeasible(machines[start],players, test ):
                    update = False
                    break

            if update:
                start += 1


    return allocation

#fits players into machines using RandomFit
def RandomFit(machines,players,taskList):
    print "random fit"
    n = shape(players)[0]
    m = shape(machines)[0]
    allocation = zeros(n*m)

    #while someone still fits
    while (taskList > 0).any():
        #pick random user based on taskList size
        probs = taskList / sum(taskList)
        user = randomChoice(probs)
        fits = False

        if taskList[user] > 0:
            #pick a random machine
            mach = random.randint(0,m)
            s = 0

            while s < m:
                test = allocation[ mach*n : (mach+1)*n ].copy()
                test[user] += 1
                if functions.isFeasible(machines[mach],players, test ):
                    allocation[mach*n+user] += 1
                    taskList[user] = taskList[user] - 1
                    fits = True
                    break
                mach = (mach+1) % m
                s += 1

            if not fits:
                taskList[user] = 0

    return allocation

#fits players into machines using BestFit
def BestFit(machines,players,taskList):
    print "best fit"
    n = shape(players)[0]
    m = shape(machines)[0]
    allocation = zeros(n*m)

    #figure out the scores per machine per player
    scores = findScores(machines,players)

    #while someone still fits
    while (taskList > 0).any():
        #pick random user based on taskList size
        probs = taskList / sum(taskList)
        user = randomChoice(probs)

        if taskList[user] > 0:
            fits = False

            s = 0
            while s < m:
                #pick the machine with the best score
                mach = argmax(scores[user])
                test = allocation[ mach*n : (mach+1)*n ].copy()
                test[user] += 1

                if functions.isFeasible(machines[mach],players, test ):
                    allocation[mach*n+user] += 1
                    taskList[user] = taskList[user] - 1
                    fits = True
                    break

                s += 1
                scores[user][mach] = -2


            if not fits:
                taskList[user] = 0

    return allocation


def bigMachineDRF(machines,players,machinesPerType, fits = None):

    numberOfMachines = machines.shape[0]

    bigMachine = sum(machines * transpose(machinesPerType), axis = 0)

    machinesLarge = dot( ones((machinesPerType[0][0],1)) , [machines[0]] )
    for j in range(1,numberOfMachines):
        #print ones((1,machinesPerType[0][j] - 1)).shape
        #print dot( ones((machinesPerType[0][j] - 1,1)) , [machines[j]] )

        machinesLarge = vstack( ( machinesLarge, dot( ones((machinesPerType[0][j],1)) , [machines[j]] ) ) )


    #print `machines`
    #print `players`
    #print machinesPerType
    #print machinesLarge


    #Find DRF solution in big machine
    [allocation,tasks] = findDRF(bigMachine,players)

    #Round it down
    tasks = floor(tasks)

    if fits is None:
        #Allocate based on different fitting algorithms
        firstFitAllocation = FirstFit(machinesLarge,players,tasks.copy())
        randomFitAllocation =  RandomFit(machinesLarge,players,tasks.copy())
        bestFitAllocation = BestFit(machinesLarge,players,tasks.copy())

        return [firstFitAllocation,randomFitAllocation,bestFitAllocation]

    elif fits == 0:
        bestFitAllocation = BestFit(machinesLarge,players,tasks.copy())
        return bestFitAllocation

    #print "FF: " + `firstFitAllocation`
    #print "RF: " + `randomFitAllocation`
    #print "BF: " + `bestFitAllocation`

    #for j in range(0,machinesLarge.shape[0]):
    #    print "FF PO " + `functions.isParetoOptimal(machinesLarge[j],players,firstFitAllocation[j*numberOfPlayers:(j+1)*numberOfPlayers])`
    #    print "RF PO " + `functions.isParetoOptimal(machinesLarge[j],players,randomFitAllocation[j*numberOfPlayers:(j+1)*numberOfPlayers])`
    #    print "BF PO " + `functions.isParetoOptimal(machinesLarge[j],players,bestFitAllocation[j*numberOfPlayers:(j+1)*numberOfPlayers])`


    #calculate utilities
    #utilitiesFF = zeros(numberOfPlayers)
    #utilitiesRF = zeros(numberOfPlayers)
    #utilitiesBF = zeros(numberOfPlayers)
    #for i in range(0,numberOfPlayers):
    #    for j in range(0,machinesLarge.shape[0]):
    #        utilitiesFF[i] += firstFitAllocation[j*numberOfPlayers + i]
    #        utilitiesRF[i] += randomFitAllocation[j*numberOfPlayers + i]
    #        utilitiesBF[i] += bestFitAllocation[j*numberOfPlayers + i]
    #print "Utilities FF: " + `utilitiesFF`
    #print "Utilities RF: " + `utilitiesRF`
    #print "Utilities BF: " + `utilitiesBF`