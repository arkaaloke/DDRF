from gurobipy import *
from gurobipy.gurobipy import *
from numpy import *
from numpy import linalg
import functions


def findCornersAndCoefficients(machine,players,cdrf,tight,x,seed):
    E = tight
    Ep = array([[]])

    a = findCorners(machine,players,x,seed,E,Ep)

    if allclose(a,cdrf):
        return [ a , 1]


    try:
        lambdas = linalg.solve(transpose(a),cdrf)
    except Exception as e:
        #print "Wrong shape"
        #print `a`
        [lambdas,res,r,s] = linalg.lstsq(transpose(a),cdrf)

        if allclose( dot(lambdas,a) , cdrf ):

            #print "all close"
            #print `a`
            while not( shape(a)[0] == shape(players)[0] ):
                a = vstack([ a, zeros( shape(players)[0] ) ])
                lambdas = hstack( [ lambdas , 0 ] )

            return [ a , lambdas]

        else:
            print "Wrong solution found"
            print "players = " + `players`
            print "machine = " + `machine`
            sys.exit()


    return [ a , lambdas]

def findCorners(machine,players,x,seed,E,Ep):

        n = shape(players)[0]
        c = 0

        #find corners
        a = CO(machine,players,E,Ep,seed)

        if allclose(x,a):
            return a

        #D = findD(machine,players,x,a)

        A = vstack( [transpose(players) , -diag(ones(n))] )
        b = hstack( [ machine , zeros(n) ] )
        Au = dot( A, (x-a) )
        b_ = b - dot( A, x )
        positive = argwhere( Au > 1e-10 )
        numbers = divide( b_[positive]  , Au[positive] )
        #print numbers

        try:
            t = min( abs(numbers) )
        except Exception as e:
            t = 0

        z = x + t*(x-a)


        #z = dot(1 + D,x) - dot(D,a)
        #print "new z is feasible: " + `functions.isFeasible(machine,players,z)`
        #print "new a: " + `a`


        E_new = argwhere( abs(dot( transpose(players) , z ) - machine) < 1e-8 )
        Ep_new = argwhere( abs(z) < 1e-8 )
        E = append(E, E_new )
        Ep = append(Ep , Ep_new )

        a = vstack( (a, findCorners(machine,players,z,seed,E,Ep) ) )
        return a


def CO(machine,players,E,Ep,seed):

        numberOfPlayers = shape(players)[0]
        numberOfResources = shape(players)[1]

        m = Model("mip1")
        m.setParam("Presolve",0)
        m.setParam("LogToConsole", 0)

        # Create variables
        x = [Var for count in range(numberOfPlayers)]

        for i in range(0, numberOfPlayers):
            x[i] = m.addVar(vtype=GRB.CONTINUOUS, name="x" + `i`)

        # Integrate new variables
        m.update()

        # Add constraints
        for j in range(0, numberOfResources):
              expr = players[0, j] * x[0]
              for i in range(1, numberOfPlayers):
                   expr.add(players[i, j] * x[i])

              if (j == E).any():
                m.addConstr(expr, GRB.EQUAL, machine[j], "c" + `j`)
              else:
                m.addConstr(expr, GRB.LESS_EQUAL, machine[j], "c" + `j`)

        for i in range(0, numberOfPlayers):
            if (i == Ep).any():
                m.addConstr( x[i] , GRB.EQUAL, 0 , "d" + `i` )


        random.seed(seed)
        dir = random.random(numberOfPlayers)

        # Set objective
        m.setObjective(LinExpr(dir, x), GRB.MAXIMIZE)

        m.optimize()

        solution = ones(numberOfPlayers)
        for i in range(0, numberOfPlayers):
                solution[i] = m.getVarByName("x" + `i`).x

        return solution

