from gurobipy.gurobipy import *
from gurobipy import *
import numpy
from numpy.linalg import *
import cvxopt


#Computes player stand alones
def computeStandAlones(machines,players):
    numberOfMachines = machines.shape[0]
    numberOfPlayers = players.shape[0]
    sa = numpy.zeros( (numberOfMachines, numberOfPlayers) )

    for j in range(0,numberOfMachines):
        for i in range(0,numberOfPlayers):
            sa[j][i] =  min(numpy.floor(numpy.divide(machines[j],players[i])))

    return sa

#checks if allocation z is feasible. Works both for long and normal format
def isFeasible(machines,players,z):

    if ( numpy.dot(numpy.transpose(players), numpy.transpose(z) ) - numpy.transpose(machines) <= 1e-10 ).all():
        return True
    else:
        return False


def cdrfFind(machines,players,ks,machinesPerType,requests):

        numberOfPlayers = numpy.shape(players)[0]
        numberOfResources = numpy.shape(players)[1]
        numberOfMachines = numpy.shape(machines)[0]


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
                        expr.addTerms(players[i, k] , x[j*numberOfPlayers + i])

                    m.addConstr(expr, GRB.LESS_EQUAL, machines[j][k], "c" + `j` + "," + `k`)
                    #print(expr , machines[j][k])


        for i in range(0, numberOfPlayers):
            expr = machinesPerType[0][0] * x[i]
            for j in range(1, numberOfMachines):
                expr.addTerms( machinesPerType[0][j] , x[j*numberOfPlayers + i] )

            m.addConstr(expr, GRB.EQUAL, a * ks[i] , "d" + `i`)
            #print(expr , a * ks[i])
            if requests.any():
                m.addConstr(expr, GRB.LESS_EQUAL, requests[i] , "f" + `i`)



        # Set objective
        m.setObjective(LinExpr(1, a), GRB.MAXIMIZE)

        m.optimize()

        solution = numpy.zeros(numberOfPlayers*numberOfMachines)
        for j in range(0, numberOfMachines):
            for i in range(0, numberOfPlayers):
                solution[j*numberOfPlayers + i] = m.getVarByName("x_" + `j` + "," + `i`).x

        #print "SOLUTION = ", solution

        return [ m.getVarByName("a").x, solution ]



def cdrfFind_new(machines,players,ks,machinesPerType,requests):

        numberOfPlayers = numpy.shape(players)[0]
        numberOfResources = numpy.shape(players)[1]
        numberOfMachines = numpy.shape(machines)[0]

        if requests.any():
            A = numpy.zeros((numberOfMachines*numberOfResources + 3*numberOfPlayers, numberOfPlayers*numberOfMachines+1))
            b = numpy.zeros(numberOfMachines*numberOfResources + 3*numberOfPlayers)
        else:
            A = numpy.zeros((numberOfMachines*numberOfResources, numberOfPlayers*numberOfMachines+1))
            A_eq = numpy.zeros((numberOfPlayers,numberOfPlayers*numberOfMachines+1))
            b = numpy.zeros(numberOfMachines*numberOfResources)
            b_eq = numpy.zeros(numberOfPlayers)

        f = numpy.zeros(numberOfPlayers*numberOfMachines+1)
        f[numberOfPlayers*numberOfMachines] = -1

        for j in range(0,numberOfMachines):
            for k in range(0, numberOfResources):

                    b[j*numberOfMachines + k] = machines[j][k]
                    for i in range(0,numberOfPlayers):
                        A[j*numberOfMachines + k][ j*numberOfPlayers + i] = players[i, k]


        for i in range(0,numberOfPlayers):
            A_eq[i, numberOfPlayers*numberOfMachines] = - ks[i]

            for j in range(0,numberOfMachines):
                A_eq[i][j*numberOfPlayers + i] = machinesPerType[0][j]

        #print A
        #print A_eq
        #print b
        #print b_eq

        A = cvxopt.matrix(A)
        b = cvxopt.matrix(b)
        A_eq = cvxopt.matrix(A_eq)
        b_eq = cvxopt.matrix(b_eq)
        c = cvxopt.matrix(f)

        sol = cvxopt.solvers.lp(c,A,b,A_eq,b_eq)
        solution = numpy.array(sol['x'])
        #print(sol['x'])

        return [ solution[numberOfPlayers*numberOfMachines] , solution[0:numberOfPlayers*numberOfMachines,0] ]

def isParetoOptimal(machine,players,row):
    leftOver = abs( numpy.dot(numpy.transpose(players), numpy.transpose(row) ) - numpy.transpose(machine) )
    for user in players:
        if ( user <= leftOver + 1e-10 ).all():
            return False

    return True

#checks if all z's are PO
def checkIfParetoOptimal(machine,players,z):
    #for every z^t increase some dimension i by 1 and check if feasible
    for row in z:
        if not isFeasible(machine,players,row):
            print("Not feasible row = ",row)


        for i in range(0,len(row)):
            temp = numpy.copy(row)
            temp[i] += 1
            if isFeasible(machine,players,temp):
                print("Not PO row= ",row)
                print("Feasible:",temp)

    return True


#checks if z is PO
#def isParetoOptimal(machine,players,row):
#    for i in range(0,len(row)):
#            temp = copy(row)
#            temp[i] += 1
#            if isFeasible(machine,players,temp):
#                return False
#     return True