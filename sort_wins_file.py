import os
import sys

diff = []
lines = open(sys.argv[1]).readlines()

for line in lines:
    d_mem = float(line.strip().split("(")[1].strip().split(",")[0].strip())
    d_cpu = float(line.strip().split(",")[1].strip().split(")")[0].strip())

    d = d_mem **2 + d_cpu **2
    
    #print line.strip(),d_mem, d_cpu, d
    #print (line.strip().split("(")[-1].strip())
    #print line.strip().split("(")[-1].strip().split(",")
    #print "\n\n\n"
    diff.append((d, line.strip()))

diff = sorted(diff, key=lambda k:k[0], reverse=True)

#print diff

for d in diff:
    print d[1].strip()

    
