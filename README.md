# ParallelBlocking

The source code that is used in:

V. Efthymiou, G. Papadakis, G. Papastefanatos, K. Stefanidis, T. Palpanas: 
Parallel Meta-blocking: Realizing Scalable Entity Resolution over Large, Heterogeneous Data. Big Data Conference 2015.

You are asked to properly cite this paper (e.g., as above), in case you use this code.

The blocking techniques were originally introduced in:

George Papadakis, Ekaterini Ioannou, Claudia Niederée, Peter Fankhauser:
Efficient entity resolution for large heterogeneous information spaces. WSDM 2011: 535-544
(for Token Blocking)

George Papadakis, Ekaterini Ioannou, Themis Palpanas, Claudia Niederée, Wolfgang Nejdl:
A Blocking Framework for Entity Resolution in Highly Heterogeneous Information Spaces. IEEE Trans. Knowl. Data Eng. 25(12): 2665-2682 (2013)
(for Attribute Clustering Blocking)

George Papadakis, Ekaterini Ioannou, Claudia Niederée, Themis Palpanas, Wolfgang Nejdl:
Beyond 100 million entities: large-scale blocking-based resolution for heterogeneous data. WSDM 2012: 53-62
(for PrefixInfixSuffix Blocking)


The code is written in Java version 7, using Apache Hadoop, version 1.2.0. All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes.
