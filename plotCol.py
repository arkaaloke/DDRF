import os
import sys
import matplotlib.pyplot as plt
 
f_suffix_name = sys.argv[1].strip()
col = int(sys.argv[2].strip())

lines_drf = open("drf." + f_suffix_name).readlines()
lines_ddrf = open("ddrf." + f_suffix_name).readlines()
lines_drf_twice = open("drf_twice." + f_suffix_name).readlines()

drf_data = []
ddrf_data = []
drf_twice_data = []

col_name = lines_drf[0].strip().split(",")[col].strip()

for line in lines_drf[1:]:
	parts = line.strip().split(",")
	t = int(parts[0].strip())
	v = float(parts[col].strip())

	drf_data.append( (t,v) )
print "Read DRF data"

for line in lines_ddrf[1:]:
	parts = line.strip().split(",")
	t = int(parts[0].strip())
	v = float(parts[col].strip())

	ddrf_data.append( (t,v) )

print "Read DDRF data"

for line in lines_drf_twice[1:]:
	parts = line.strip().split(",")
	t = int(parts[0].strip())
	v = float(parts[col].strip())

	drf_twice_data.append( (t,v) )

print "Read DRF_twice data"

plt.figure()
plt.plot( [ v[0] for v in drf_data ] , [ v[1] for v in drf_data ] , label="DRF" )
plt.plot( [ v[0] for v in ddrf_data ] , [ v[1] for v in ddrf_data ] , label="DDRF" )
plt.plot( [ v[0] for v in drf_twice_data ] , [ v[1] for v in drf_twice_data ] , label="DRF_twice" )
plt.ylabel( col_name )
plt.xlabel( "Time(s) " )
plt.legend()
plt.savefig(f_suffix_name + "-" + str(col_name) + ".png" , dpi=200)
#plt.show()

