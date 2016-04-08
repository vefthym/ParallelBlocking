# ParallelBlocking

The source code that is used in:

Vasilis Efthymiou, Kostas Stefanidis, Vassilis Christophides: Big data entity resolution: From highly to somehow similar entity descriptions in the Web. Big Data 2015: 401-410.

You are asked to properly cite this paper (e.g., as above), in case you use this code.

For more information on the subject, we recommend visitng our project's website: http://csd.uoc.gr/~vefthym/minoanER/
or reading our book (http://www.morganclaypool.com/doi/10.2200/S00655ED1V01Y201507WBE013):
Vassilis Christophides, Vasilis Efthymiou, Kostas Stefanidis:
Entity Resolution in the Web of Data. Synthesis Lectures on the Semantic Web: Theory and Technology, Morgan & Claypool Publishers 2015

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


The code is written in Java version 7, using Apache Hadoop, version 1.2.0. All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes. The cluster was provided by GRNET's ~okeanos service (https://okeanos.grnet.gr/home/).
