from gurobipy.gurobipy import *
from gurobipy import *
from numpy import *
from numpy.linalg import *
import sys


#Computes player stand alones
def computeStandAlones(machines,players):
    numberOfMachines = machines.shape[0]
    numberOfPlayers = players.shape[0]
    sa = zeros( (numberOfMachines, numberOfPlayers) )

    for j in range(0,numberOfMachines):
        for i in range(0,numberOfPlayers):
            sa[j][i] =  min(floor(divide(machines[j],players[i])))

    return sa

#checks if allocation z is feasible. Works both for long and normal format
def isFeasible(machines,players,z):

    if ( dot(transpose(players), transpose(z) ) - transpose(machines) <= 1e-10 ).all():
        return True
    else:
        return False

def cdrfFind(machines,players,ks,machinesPerType,requests):

        numberOfPlayers = shape(players)[0]
        numberOfResources = shape(players)[1]
        numberOfMachines = shape(machines)[0]


        m = Model("mip1")
        m.setParam("LogToConsole", 0)

        # Create variables
        x = [Var for count in range(numberOfPlayers*numberOfMachines)]


        for j in range(0, numberOfMachines):
            for i in range(0, numberOfPlayers):
                x[j*numberOfPlayers + i] = m.addVar(vtype=GRB.CONTINUOUS, name="x_" + `j` + "," + `i`)

        a = m.addVar(vtype = GRB.CONTINUOUS, name = "a")

        # Integrate new variables
        m.update()

        # Add constraints
        for j in range(0,numberOfMachines):
            for k in range(0, numberOfResources):

                    expr = players[0, k] * x[j*numberOfPlayers]

                    for i in range(1, numberOfPlayers):
                        expr.add(players[i, k] * x[j*numberOfPlayers + i])

                    m.addConstr(expr, GRB.LESS_EQUAL, machines[j][k], "c" + `j` + "," + `k`)

        for i in range(0, numberOfPlayers):
            expr = machinesPerType[0][0] * x[i]
            for j in range(1, numberOfMachines):
                expr.add( machinesPerType[0][j] * x[j*numberOfPlayers + i] )

            m.addConstr(expr, GRB.EQUAL, a * ks[i] , "d" + `i`)
            if requests.any():
                m.addConstr(expr, GRB.LESS_EQUAL, requests[i] , "f" + `i`)



        # Set objective
        m.setObjective(LinExpr(1, a), GRB.MAXIMIZE)

        m.optimize()

        solution = zeros(numberOfPlayers*numberOfMachines)
        for j in range(0, numberOfMachines):
            for i in range(0, numberOfPlayers):
                solution[j*numberOfPlayers + i] = m.getVarByName("x_" + `j` + "," + `i`).x

        return [ m.getVarByName("a").x, solution ]

def isParetoOptimal(machine,players,row):
    leftOver = abs( dot(transpose(players), transpose(row) ) - transpose(machine) )
    for user in players:
        if ( user <= leftOver + 1e-10 ).all():
            return False

    return True

#checks if all z's are PO
def checkIfParetoOptimal(machine,players,z):
    #for every z^t increase some dimension i by 1 and check if feasible
    for row in z:
        if not isFeasible(machine,players,row):
            print "Not feasible row = " + `row`


        for i in range(0,len(row)):
            temp = copy(row)
            temp[i] += 1
            if isFeasible(machine,players,temp):
                print "Not PO row= " + `row`
                print "Feasible:" + `temp`

    return True


#checks if z is PO
#def isParetoOptimal(machine,players,row):
#    for i in range(0,len(row)):
#            temp = copy(row)
#            temp[i] += 1
#            if isFeasible(machine,players,temp):
#                return False
#     return True