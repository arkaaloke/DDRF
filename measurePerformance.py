from numpy import *

#functions that measure performance of different heuristics

def findUtilities(allocation,numberOfPlayers,machinesPerType, sa = None):
    utilities = zeros(numberOfPlayers)
    for i in range(0,numberOfPlayers):
            for j in range( 0,sum(machinesPerType) ):
                utilities[i] += allocation[j*numberOfPlayers + i]

    if not sa is None:
        utilities = divide(utilities, sa)
    return utilities

def findUtilization(allocation,players,machinesPerType,machines, perType = False):

    numberOfPlayers = players.shape[0]
    bigMachine = sum(machines * transpose(machinesPerType), axis = 0)

    utilized = zeros(players.shape[1])

    for j in range(0, sum(machinesPerType) ):
        alloc = allocation[j*numberOfPlayers : (j+1)*numberOfPlayers]
        utilized += dot(alloc,players)

    return divide( utilized , bigMachine )
