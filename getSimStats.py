import pstats

p = pstats.Stats('stats')
#p.sort_stats('tottime').print_stats(20)
#p.sort_stats('tottime').print_stats()
p.sort_stats('tottime').print_stats()
