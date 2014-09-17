UriPositionAnalysis
===================

Analysis of URI positions in RDF triples.

Description
-----------
This is a collection of python scripts and Hadoop mapreduce jobs to identify on which position in an RDF triple which URI occurs how often.

Usage
-----

1. Download the an gzipped N-Quad data set [like this](http://data.dws.informatik.uni-mannheim.de/lodcloud/2014/ISWC-RDB/dump.nq.gz).
2. Change the 'Config.py' entries.
3. Excecute the python script 'ExtractURIs.py' in the 'python' folder.
4. Use the newly generated file as input for the Hadoop mapreduce job "UriPositionAnalysisRunner" in the 'Java' folder.
Additionally:
* You can use the 'CountURIs.py" python script to do some statistical analysis like how many URIs occur how often in which position.

Todos
-----
* code is not well documented, sorry. I will change that soon.
* some variable names are not good chosen, I am aware of that.
