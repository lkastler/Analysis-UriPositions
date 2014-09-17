'''
Created on Sep 16, 2014

@author: lkastler
'''
import Config
import re
import gzip
import logging as log

def readFile(fileName):
	f = gzip.open(fileName)
	line = f.readline()
	
	while line != "":
		yield line
		line = f.readline()
		
	f.close()

def splitNQ(nq, out):
	split = nq.split(" ")
	if len(split) == 5:
		getURI(split[0], "s", out)
		getURI(split[1], "p", out)
		getURI(split[2], "o", out)

def getURI(resource, position, out):	
	#log.debug(resource)
	if re.match("^\<(.+?)\>$", resource) != None:
		#log.debug("found: " + resource)
		out.write(resource + " " + position + "\n")

def main():
	log.basicConfig(level=log.DEBUG)
	log.info("starting")
	
	out = open(Config.outPut, "w")
		
	for line in readFile(Config.inPut):
		splitNQ(line, out)
	out.flush()
	out.close()
	log.info("end")

if __name__ == "__main__":
	main()