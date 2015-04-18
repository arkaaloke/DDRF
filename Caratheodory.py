from gurobipy import *
from gurobipy.gurobipy import *
import numpy
from numpy import linalg
import sys
import cvxopt
import functions
import time


def findCornersAndCoefficients(machine,players,cdrf,x,seed):

    E = numpy.argwhere( abs(numpy.dot( numpy.transpose(players) , cdrf ) - machine) < 1e-10 )
    Ep = numpy.array([[]])

    #a = findCorners(machine,players,x,seed,E,Ep)
    a = findCornersAlternative(machine,players,x,seed,E,Ep)
    #print "alt ", a

    if len(a.shape) == 1:
        a = [a]

    if numpy.allclose(a,cdrf):
        return [ a , 1]

    try:
        lambdas = linalg.solve(numpy.transpose(a),cdrf)
    except Exception as e:
        #print "Wrong shape"
        #print `a`
        [lambdas,res,r,s] = linalg.lstsq(numpy.transpose(a),cdrf)

        if numpy.allclose( numpy.dot(lambdas,a) , cdrf ):

            #print "all close"
            #print `a`
            while not( numpy.shape(a)[0] == numpy.shape(players)[0] ):
                a = numpy.vstack([ a, numpy.zeros( numpy.shape(players)[0] ) ])
                lambdas = numpy.hstack( [ lambdas , 0 ] )

            return [ a , lambdas]

        else:
            print("Wrong solution found")
            print("players = " ,players)
            print("machine = " ,machine)
            print("lamdbas dot a " , numpy.dot(lambdas,a))
            print("cdrf " , cdrf)
            sys.exit()


    return [ a , lambdas]

def findCorners(machine,players,x,seed,E,Ep):

        n = numpy.shape(players)[0]
        c = 0

        #find corners
        a = CO_new(machine,players,E,Ep,seed)

        #print "aaassasfs "

        if numpy.allclose(x,a):
            return a

        #D = findD(machine,players,x,a)

        A = numpy.vstack( [numpy.transpose(players) , -numpy.diag(numpy.ones(n))] )
        b = numpy.hstack( [ machine , numpy.zeros(n) ] )
        Au = numpy.dot( A, (x-a) )
        b_ = b - numpy.dot( A, x )
        positive = numpy.argwhere( Au > 1e-10 )
        numbers = numpy.divide( b_[positive]  , Au[positive] )
        #print numbers

        try:
            t = min( abs(numbers) )
        except Exception as e:
            t = 0


        z = x + t*(x-a)

        E_new = numpy.argwhere( abs(numpy.dot( numpy.transpose(players) , z ) - machine) < 1e-10 )
        Ep_new = numpy.argwhere( abs(z) < 1e-8 )
        E = numpy.append(E, E_new )
        Ep = numpy.append(Ep , Ep_new )

        #print "E " , E
        #print "Ep " , Ep

        a = numpy.vstack( (a, findCorners(machine,players,z,seed,E,Ep) ) )

        return a



def findCornersAlternative(machine,players,x,seed,E,Ep):

        n = numpy.shape(players)[0]
        c = 0

        #find corners
        a = CO(machine,players,E,Ep,seed)
        out = a

        while not numpy.allclose(x,a):
                #print(x)

                #print "here ", a

                #D = findD(machine,players,x,a)
                A = numpy.vstack( [numpy.transpose(players) , -numpy.diag(numpy.ones(n))] )
                b = numpy.hstack( [ machine , numpy.zeros(n) ] )
                Au = numpy.dot( A, (x-a) )
                b_ = b - numpy.dot( A, x )
                positive = numpy.argwhere( Au > 1e-10 )
                numbers = numpy.divide( b_[positive]  , Au[positive] )
                #print numbers

                try:
                    t = min( abs(numbers) )
                except Exception as e:
                    t = 0


                z = x + t*(x-a)

                E_new = numpy.argwhere( abs(numpy.dot( numpy.transpose(players) , z ) - machine) < 1e-10 )
                Ep_new = numpy.argwhere( abs(z) < 1e-8 )
                E = numpy.append(E, E_new )
                Ep = numpy.append(Ep , Ep_new )

                x = z

                #a = vstack( (a, findCorners(machine,players,z,seed,E,Ep) ) )
                a = CO(machine,players,E,Ep,seed)
                out = numpy.vstack( (out, a ) )

                #print(x)

        return out



def CO(machine,players,E,Ep,seed):

        #print("E = ", E)
        #print("Ep = ", Ep)

        numberOfPlayers = numpy.shape(players)[0]
        numberOfResources = numpy.shape(players)[1]

        m = Model("mip1")
        m.setParam("Presolve",0)
        m.setParam("LogToConsole", 0)

        # Create variables
        x = [Var for count in range(numberOfPlayers)]
        expr = [ LinExpr for count in range(numberOfResources)]

        #print expr[0]

        for i in range(0, numberOfPlayers):
            x[i] = m.addVar(vtype=GRB.CONTINUOUS, name="x" + `i`)

        # Integrate new variables

        m.update()
        #print "update time = " , time.time() - t


        # Add constraints
        for j in range(0, numberOfResources):

              #t = time.time()
              expr[j] = players[0, j] * x[0]
              #expr[j].addTerms(players[:,j] , x )
              for i in range(1, numberOfPlayers):
                   expr[j].addTerms(players[i, j] , x[i])
              #print "call " , time.time() - t


              if (j == E).any():
                m.addConstr(expr[j], GRB.EQUAL, machine[j], "c" + `j`)
              else:
                m.addConstr(expr[j], GRB.LESS_EQUAL, machine[j], "c" + `j`)


        for i in range(0, numberOfPlayers):
            if (i == Ep).any():
                m.addConstr( x[i] , GRB.EQUAL, 0 , "d" + `i` )



        numpy.random.seed(seed)
        dir = numpy.random.random(numberOfPlayers)

        # Set objective
        m.setObjective(LinExpr(dir, x), GRB.MAXIMIZE)


        m.optimize()


        solution = numpy.ones(numberOfPlayers)
        for i in range(0, numberOfPlayers):
                solution[i] = m.getVarByName("x" + `i`).x


        return solution


def CO_new(machine,players,E,Ep,seed):

        #print("E = ", E)
        #print("Ep = ", Ep)

        numberOfPlayers = numpy.shape(players)[0]
        numberOfResources = numpy.shape(players)[1]


        A = numpy.zeros((2*numberOfResources + 2*numberOfPlayers,numberOfPlayers))
        b = numpy.zeros(2*numberOfResources + 2*numberOfPlayers)

        for k in range(0, numberOfResources):
            for i in range(0, numberOfPlayers):
                A[2*k][i] = players[i, k]
                b[2*k] = machine[k]

                if (k == E).any():
                    A[2*k][i] = - players[i, k]
                    b[2*k] = - machine[k]
                else:
                    A[2*k+1][i] = players[i, k]
                    b[2*k+1] = machine[k]


        for i in range(0, numberOfPlayers):
            if (i == Ep).any():
                A[2*numberOfResources+2*i] = 1
                A[2*numberOfResources+2*i+1] = -1


        numpy.random.seed(seed)
        dir = numpy.random.random(numberOfPlayers)

        A = cvxopt.matrix(A)
        b = cvxopt.matrix(b)
        c = cvxopt.matrix(dir)
        sol = cvxopt.solvers.lp(c,A,b)
        solution = numpy.array(sol)

        return solution


