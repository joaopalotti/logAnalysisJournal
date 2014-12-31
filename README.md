Log Analysis
============

Basic tools to analyze log files. Here we are going to explain step by step
how to analyse the AOL logs.

Required Files
==============

Please, download the AOL dataset from http://www.gregsadetsky.com/aol-data/
We also will need dmoz data: http://rdf.dmoz.org/rdf/content.rdf.u8.gz (you can contact me directly, as this step can be very boring)

Requered Dependences
====================

* python 2.7
* numpy
* matplotlib
* pandas
* simplejson

These dependences can be downloaded using pip or your system package manager:

> sudo apt-get install python-numpy python-matplotlib python-pandas python-simplejson
> sudo yum install numpy python-matplotlib python-pandas python-simplejson


How To Use
===========

Preprocessing steps
-------------------
 * Extract and configure dmoz data (I can help you doing it as well - aolPreprocess folder)
 * If necessary, get the Metamap mappings: I also have it done, but if you want this step may take you up to one week. (some gigas of download)
 * Extract the AOL data and combine it with the Metamap mappings (umls folder)

Log Analysis
------------
Once everything is ready, just change the datapaths in main.py and run it.




