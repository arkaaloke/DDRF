from numpy import *
import functions
import Caratheodory
import scipy.linalg
import sys, traceback
import BigMachineLP
import Rounding

def allocateLeftoverMachines(players, al_types, m , z, cdrf ):
    inc = 0

    x = divide(cdrf - dot(al_types / m,z), cdrf)

    for i in range(0,players.shape[0]):
            if math.isnan(x[i]) or math.isinf(x[i]):
                x[i] = 0

    #find who's worse off with the current state of affairs
    worse = argmax(x)

    return argmax(z[:,worse])


def computeCDRF(smart_rounding,machines,players,machinesPerType,c,flag, requests = array([])):

    numberOfMachineTypes = machines.shape[0]
    numberOfPlayers = players.shape[0]
    numberOfResources = machines.shape[1]

    S = zeros((numberOfMachineTypes,numberOfPlayers,numberOfPlayers))
    L = zeros((numberOfMachineTypes,numberOfPlayers))
    cdrfIndividual = zeros((numberOfMachineTypes,numberOfPlayers))

    machines_long = zeros(numberOfMachineTypes*numberOfResources)
    for i in range(0,numberOfMachineTypes):
        for j in range(0,numberOfResources):
            machines_long[i*numberOfResources + j] = machines[i][j]

    players_long = players
    for i in range(0,numberOfMachineTypes-1):
        players_long = scipy.linalg.block_diag(players_long, players)


    sa = functions.computeStandAlones(machines, players)

    sa_long = zeros(numberOfMachineTypes*numberOfPlayers)
    for i in range(0,numberOfMachineTypes):
        for j in range(0,numberOfPlayers):
            sa_long[i*numberOfPlayers + j] = sa[i][j]


    sa =  sa * transpose(machinesPerType)
    ks = sum(sa, axis=0)

    #Find CDRF on the LP relaxation
    [a,allocation] = functions.cdrfFind(machines,players,ks,machinesPerType, requests)

    #print "al = " , `allocation`
    #print "a = " , `a`

    #CDRF = dot(a,ks)
    #print "CDRF = ", CDRF

    #Find corners and lambdas and round them down
    try:
        for j in range(0,numberOfMachineTypes):

                    print "machine " + `j`
                    CDRF_m = allocation[ j*numberOfPlayers : (j+1)*numberOfPlayers ]
                    cdrfIndividual[j] = CDRF_m

                    #print "cdrf ",j, CDRF_m
                    #print "aloc feas = " , dot( transpose(players) , CDRF_m )

                    tight = argwhere( abs(dot( transpose(players) , CDRF_m ) - machines[j]) < 1e-10 )

                    [ z , lambdas ] = Caratheodory.findCornersAndCoefficients(machines[j],players,CDRF_m ,tight,CDRF_m,c)

                    #print "z = " , z
                    #print "lambdas = " , lambdas

                    #print "Z = " +`z`
                    #support = []
                    #print "Support for machine type " +`j`+ " is: "
                    #for i in range(0,numberOfPlayers):
                    #    print argwhere(z[i] > 1e-10)
                    #    if argwhere(z[i] > 1e-10).any():
                    #        support.append(argwhere(z[i] > 1e-10))

                    #print "support = " +`support`


                    #add row of zeros to get rid of bug in rounding
                    z = vstack( ( z, zeros(numberOfPlayers) ) )
                    lambdas = append(lambdas, 0)

                    print z
                    #Round z down
                    if smart_rounding:
                        z_imp = Rounding.improveWorst(players,machines[j],z,lambdas,CDRF_m)
                    else:
                        z_imp = Rounding.trivialRounding(z)
                    print z_imp

                    #remove row of zeros
                    z = delete( z , -1 , 0 )
                    z_imp = delete( z_imp, -1 , 0 )
                    lambdas = delete( lambdas, -1 , 0 )

                    #print "z_imp = " , z_imp
                    #print "lam = " , lambdas

                    #store it
                    S[j] = z_imp
                    L[j] = lambdas

                    #test solution
                    if not allclose( dot(lambdas,z) , CDRF_m ):
                        print "lambdas are wrong!"
                        print "players = " + `players`
                        print "machine = " + `machines`
                        sys.exit()

                    if ( lambdas > 1 + 1e-10).any():
                        print "lambdas greater than 1 " + `sum(lambdas)`
                        print "lambdas > 1: " + `lambdas>1`
                        print "players = " + `players`
                        print "machine = " + `machines[j]`
                        sys.exit()

                    if (lambdas < -1e-10).any():
                        print "lambdas smaller than 0"
                        print "lambdas = " + `lambdas`
                        print "players = " + `players`
                        print "machines = " + `machines`
                        sys.exit()

                    if (abs(sum(lambdas) - 1) > 1e-10):
                        print "lambdas don't add up to 1"
                        print "players = " + `players`
                        print "machines = " + `machines`
                        print "sum = ", sum(lambdas)
                        sys.exit()

    except Exception as ex:
        print "Error! " + `ex`
        traceback.print_exc(file=sys.stdout)
        print "players = " + `players`
        print "machines = " + `machines`
        sys.exit()

    m = sum(machinesPerType)
    #print "Total number of machines: " + `m`

    allocation = []

    #print "S = " + `S`

    #allocate \ceil( (1-e)m lambda_t ) times z_t
    for j in range(0,numberOfMachineTypes):

        mt = machinesPerType[0][j]
        #e = 2n / m
        e = (2.0*numberOfPlayers) / mt

        #fix lambdas that might be wrong
        if not abs( sum(L[j]) - 1 ) < 1e-10:
            L[j] = L[j] / sum(L[j])

        #print "L[j] = " + `L[j]`
        #print "the number of machines of type " + `j` + " is " + `mt`

        al_types = zeros(numberOfPlayers)

        for t in range(0,numberOfPlayers):
            if flag == 1:
                al_types[t]  = max( ceil( (1-e) * mt * L[j][t] ) , 0 )
            else:
                al_types[t] = max( floor( mt * L[j][t] ) , 0 )

        #print "altypes: " , al_types
        #print "altypes1: ", al_types1
        #print sum(al_types), sum(al_types1) , mt

        #print "machine for this type: "+ `mt`, "unallocated: " + `(mt - sum(al_types))`

        if sum(al_types) > mt:
            print "Allocated more that we should!"
            sys.exit()

        while sum(al_types) < mt:
            inc = allocateLeftoverMachines( players,al_types,mt , S[j], cdrfIndividual[j] )

            al_types[inc] += 1

        #print "number of times to allocate: " + `sum(al_types)`
        #print "al_types: " + `al_types`

        #do the allocation as promised
        for t in range(0,numberOfPlayers):
            for k in range(0,int(al_types[t])):
                #print "S[j][t] =  " + `S[j][t]`
                allocation = hstack( ( allocation, S[j][t] ) )


    #print "allocation = " , allocation

    return allocation

