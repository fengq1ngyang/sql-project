from collections import Counter

import collections

colors = ['red', 'blue', 'red', 'green', 'blue', 'blue']
c = collections.Counter(colors)
print(type(c))
print(Counter('abracadabra').most_common(3))
