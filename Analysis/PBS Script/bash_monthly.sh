#!/bin/bash

## loop over count, submit job to gadi with count that gets communicated to python

#for i in {0..11}
#do
   #echo "creating job $i"
   #qsub -v year=105,var=0,mo=${i},time=1 run_save_10daily_quantities_10yr.sh
#done

for i in {0..5}
do
   echo "creating job $i"
   qsub -v year=105,var=${i},mo=0,time=10 run_save_10daily_quantities_10yr_loop.sh 
done
