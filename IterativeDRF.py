from numpy import *
import functions

#computes scores for bestFit
def findScores(machines,players,ff):
    n = shape(players)[0]
    m = shape(machines)[0]
    scores = zeros((n,m))

    if ff:
        return scores

    for i in range(0,n):
        for j in range(0,m):
            #scores[i][j] = divide( dot(players[i],machines[j]) , linalg.norm(machines[j],2) * linalg.norm(players[i],2) )
            scores[i][j] = 1 - sum(divide(players[i],machines[j]))
            #print i,j,scores[i][j]

    return scores

def findScoresDyn(machinesLarge,players,allocation,user):
    n = shape(players)[0]
    m = shape(machinesLarge)[0]
    scores = zeros(m)

    for j in range(0,m):

        mach = machinesLarge[j]
        alloc = allocation[j*n: (j+1)*n ]
        p = dot(transpose(players), transpose(alloc) )

        scores[j] = 1 - sum(divide(players[user],( machinesLarge[j] - p )))

    return scores

def someoneFits(allocation, players, machinesLarge):

    numberOfPlayers = players.shape[0]

    for j in range(0, machinesLarge.shape[0] ):
        mach = machinesLarge[j]
        alloc = allocation[j*numberOfPlayers: (j+1)*numberOfPlayers ]

        if not functions.isParetoOptimal(mach,players,alloc):
            return True

    return False

def findDominantShares(players,dominantResources,allocationBig,bigMachine):
    shares = zeros(players.shape[0])

    for p in range(0, players.shape[0] ):
        #dominant share = numberOfTasks * demand / total availability
        shares[p] = ( players[p][dominantResources[p]] * allocationBig[p] ) / bigMachine[ dominantResources[p] ]

    return shares


def iterativeDRF(machines,players,machinesPerType, sa , fit_flag , flag_dyn ):

    print "iterative DRF"

    numberOfMachines = machines.shape[0]
    numberOfPlayers = players.shape[0]

    bigMachine = sum(machines * transpose(machinesPerType), axis = 0)

    print machinesPerType[0][0]
    print [machines[0]]
    machinesLarge = dot( ones((machinesPerType[0][0],1)) , [machines[0]] )
    for j in range(1,numberOfMachines):
        machinesLarge = vstack( ( machinesLarge, dot( ones((machinesPerType[0][j],1)) , [machines[j]] ) ) )

    m = machinesLarge.shape[0]


    #find dominant resources per player, using the big machine
    dominantResources = divide(players,bigMachine).argmax(axis=1)


    #findScores

    #scores = findScores(machinesLarge,players)
    #print scores

    #scores
    sortedMachinesPerScore = argsort(-findScores(machinesLarge,players,fit_flag),axis=1, kind='mergesort')

    #the first machine that user i fits in
    playerFits = zeros(numberOfPlayers)

    active = array(range(0,numberOfPlayers))

    allocation = zeros( sum(machinesPerType) * numberOfPlayers )
    allocationBig = zeros( numberOfPlayers )


    #implement iterative drf: pick user with lowest dominant share and allocate her to the machine with the highest score that she fits in
    #for rep in range(0,reps):
    while active.any():


            #next user is user with lowest dominant share

            #find dominant shares
            shares = findDominantShares(players,dominantResources,allocationBig,bigMachine)
            #find player with min dominant share, out of the active users
            user = argwhere( shares == min(shares[active]) )[0][0]

            #sometimes the player returned is not active
            i = 0
            while (not user in active):
                i += 1
                user =  argwhere( shares == min(shares[active]) )[i][0]


            #dynamically change the score
            if flag_dyn == 1:
                sortedMachinesPerScore[i] = argsort(-findScoresDyn(machinesLarge,players,allocation,user), kind='mergesort')


            #try to fit her somewhere
            fits = False

            #for s in range(0,m):
            while playerFits[user] < m:

                #pick the machine with the best score

                #mach = argmax( scores[user] )
                mach = sortedMachinesPerScore[user][playerFits[user]]

                test = allocation[ mach*numberOfPlayers : (mach+1)*numberOfPlayers ].copy()
                test[user] += 1

                if functions.isFeasible(machinesLarge[mach],players, test ):
                    fits = True
                    allocation[mach*numberOfPlayers+user] += 1
                    allocationBig[user] += 1
                    break

                playerFits[user] += 1

                #scores[user][mach] = -inf

            if not fits:
                #print "deleting player " + `user`
                active = delete(active, argwhere(active == user) )

            #print allocationBig


    return allocation


