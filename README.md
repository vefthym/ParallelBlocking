# ParallelBlocking

The source code that is used in:

Vasilis Efthymiou, Kostas Stefanidis, Vassilis Christophides: Benchmarking Blocking Algorithms for Web Entities. IEEE Transactions on Big Data (2017).

and 

Vasilis Efthymiou, Kostas Stefanidis, Vassilis Christophides: Big data entity resolution: From highly to somehow similar entity descriptions in the Web. Big Data 2015: 401-410.


You are asked to properly cite those papers (e.g., as above), in case you use this code.

The datasets used can be downloaded from http://csd.uoc.gr/~vefthym/minoanER/datasets.html

For more information on the subject, we recommend visitng our project's website: http://csd.uoc.gr/~vefthym/minoanER/
or reading our book (http://www.morganclaypool.com/doi/10.2200/S00655ED1V01Y201507WBE013):
Vassilis Christophides, Vasilis Efthymiou, Kostas Stefanidis:
Entity Resolution in the Web of Data. Synthesis Lectures on the Semantic Web: Theory and Technology, Morgan & Claypool Publishers 2015

The blocking techniques were originally introduced in:

George Papadakis, Ekaterini Ioannou, Themis Palpanas, Claudia Niederée, Wolfgang Nejdl:
A Blocking Framework for Entity Resolution in Highly Heterogeneous Information Spaces. IEEE Trans. Knowl. Data Eng. 25(12): 2665-2682 (2013)

The code is written in Java version 7, using Apache Hadoop, version 1.2.0. All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes. The cluster was provided by GRNET's ~okeanos service (https://okeanos.grnet.gr/home/).
