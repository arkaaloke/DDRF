import os
import heapq

arr = [ i for i in range(10) ]
arr.extend( [ i for i in range(5) ] )

heapq.heapify(arr)
print arr
arr.remove(9)
heapq.heapify(arr)
print arr

