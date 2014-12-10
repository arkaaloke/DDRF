from numpy import *
import functions
import math

def trivialRounding(z):
    return floor(z)

def randomRounding(players,machine,z):

    n = shape(players)[0]
    z = floor(z)

    for row in z:

        element = random.randint(0,n)

        while not functions.isParetoOptimal(machine,players,row):
            temp = copy(row)
            temp[element] = temp[element] + 1

            if functions.isFeasible(machine,players,temp):
                row[element] = row[element] + 1

            element = (element+1) % n

    return z

def improveWorst(players,machine,z,l,c):

    n = shape(players)[0]
    z = floor(z)

    for row in z:
        x = divide(c - dot(l,z), c)

        for i in range(0,n):
            if math.isnan(x[i]) or math.isinf(x[i]):
                x[i] = 0

        worse = argmax(x)

        while not functions.isParetoOptimal(machine,players,row):
            temp = copy(row)
            temp[worse] = temp[worse] + 1

            if functions.isFeasible(machine,players,temp):
                row[worse] = row[worse] + 1
            else:
                x[worse] = -100
                worse = argmax(x)

    return z
