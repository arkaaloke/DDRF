import numpy
import os
import sys
import matplotlib.pyplot as plt
 
f_name = sys.argv[1].strip()
col = int(sys.argv[2].strip())

lines = open(f_name).readlines()
parts = lines[0].strip().split(",")
print "Parts : ", parts
graph_name = parts[col].strip().split('_')[-1].strip()


label_names = [ ]
num_cols = 0
j = col
reqd_cols = []
while j < len(parts) - 1:
    print "Doing : " , parts[j].strip()
    if "Total" not in parts[j].strip():
        print parts[j]
        j += 3
        continue
    print "Total in parts : ", parts[j]
    label_names.append(parts[j].strip())
    reqd_cols.append(j)
    j = j + 3

print "Required columns : ", reqd_cols

values = numpy.zeros( ( len(reqd_cols) , len(lines) - 1 ) )

times = []
linenum = 0
for line in lines[1:]:
    parts = line.strip().split(",")
    times.append( int(parts[0].strip()) )

    count = 0
    #print "Len(parts) = ", len(parts)
    for c in reqd_cols:
        #print parts[c].strip() , j , linenum
        values[count , linenum ] = float(parts[c].strip())
        count += 1
    linenum += 1

print "Number of columns : " , len(reqd_cols)
fig = plt.figure()
ax = plt.subplot(111)

for i in range( len(reqd_cols) ):
    plt.plot( times, values[i,:]  , '-', label=label_names[i] )

box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])

ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
          fancybox=True, shadow=True, ncol=5)
plt.ylabel( graph_name )
plt.xlabel( "Time(s) " )
#plt.legend()
plt.savefig(f_name.split("_")[0].strip() +  "." + str(graph_name) + ".png" , dpi=200)
#plt.show()

