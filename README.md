# Partitioned-Table-Data-Dump
Creating a partitioned table of historical data in Big Query using Bash Script. The structure of table is similar to how Google Analytics pushes data into Big Query Tables, where the suffix of the table being date (in format of YYYYMMDD).
This method is useful rather than using the flat table is the cost opimization of query to be written using this partitioned table as sub-table. A simple bash script containing a loop is run over the script so that a new table is saved for each date. (see bash script) 
