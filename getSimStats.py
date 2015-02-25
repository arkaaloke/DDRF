import pstats

p = pstats.Stats('stats')
p.sort_stats('time').print_stats(20)
#p.sort_stats(-2).print_stats()
