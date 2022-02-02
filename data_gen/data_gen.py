import random

myfile = open('data.txt', 'w')

keys = ['a','b','c','d']

for i in range(1000000):
    rand = random.randint(0, 1000000)
    line = keys[rand%4]+","+str(rand)
    myfile.write("%s\n" % line)

myfile.close()