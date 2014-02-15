estimate-pi-hadoop
==================

Compute the value of π with MapReduce

This was an experimental project, and is not
recommended for actually finding the value of π.


Details
-------

This project was based on the paper
"The BBP Algorithm for Pi" by David H. Bailey.
It is available at http://crd.lbl.gov/~dhbailey/dhbpapers/bbp-alg.pdf

This paper details a method for computing a handful
of hexadecimal digits starting at a certain place
after the decimal point. For some n number of digits requested,
I apply this method repeatedly to get the digits from 0 through n.
Each call to map() generates one handful of digits (currently set to 5).
The further this handful is from the decimal point, the more time it
takes to generate. Unfortunately, this means the total running time
grows geometrically.
