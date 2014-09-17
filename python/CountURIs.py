'''
Created on Sep 16, 2014

@author: lkastler
'''
import Config
import gzip as gz
import logging as log
from _collections import defaultdict
import pprint
from operator import itemgetter

subj = defaultdict(int)
pred = defaultdict(int)
obj = defaultdict(int)
total = defaultdict(int)

def readFile(fileName):
	f = gz.open(fileName, "r")
	line = f.readline()
	
	while line != "":
		yield line
		line = f.readline()
		
	f.close()

def main(fileName):
	global subj,pred,obj,total
	
	for line in readFile(fileName):
		split = line.rstrip().split("\t")
		if len(split) == 5:
			subj_count = split[1]
			pred_count = split[2]
			obj_count = split[3]
			total_count = split[4]
			
			subj[subj_count] = subj[subj_count] + 1
			pred[pred_count] = pred[pred_count] + 1
			obj[obj_count] = obj[obj_count] + 1
			total[total_count] = total[total_count] + 1
		else:
			log.error("not well-formed: " + line)

def sortBy(k):
	return int(k[0])

def printDict(toPrint, out):
	out.write("count \t occurrences" + '\n');
	for k, v in sorted(toPrint.iteritems(), key=sortBy, reverse=True):
		out.write(str(k) + "\t" + str(v) + "\n");
		
	out.write("\n")

def printURIs(output):
	out = open(output, "w")
	#for uri in sorted(uris, key=operator.itemgetter(1)):
	out.write("TOTAL\n")
	printDict(total, out)
	
	out.write("SUBJECT\n")
	printDict(subj, out)
	
	out.write("PREDICATE\n")
	printDict(pred, out)
	
	out.write("OBJECT\n")
	printDict(obj, out)
	
	out.flush() 
	out.close()

if __name__ == '__main__':
	log.basicConfig(level=log.DEBUG)
	log.info("start")
	main(Config.inPut2)
	
	printURIs(Config.positionCount)
	log.info("end")