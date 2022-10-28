#!/bin/bash
#PBS -P e14
#PBS -q normalbw
#PBS -l mem=250gb,ncpus=8,walltime=0:15:00,storage=gdata/ik11+gdata/hh5+gdata/x77+gdata/e14+scratch/e14,jobfs=100gb
#PBS -N save_daily_binned_heat_terms_monthly_looped
#PBS -v year,var,mo,time
#PBS -j oe

module use /g/data3/hh5/public/modules
module unload conda
module load conda/analysis3-22.07

cd  /g/data/e14/cy8964/analysis/scripts/
# set max number of time loops to run:
n_loops=12
script_dir=/g/data/e14/cy8964/analysis/scripts
echo "submitting month $count to gadi"

# call python
python3 save_10daily_quantities_xhistogram_10yr-climtas-dask.py $year $var $mo $time  #&>> output_save_10daily_quantities_xhistogram_10yr_year${year}_var${var}_mo${mo}_time${time}.txt

# increment count and resubmit:
mo=$((mo+1))
if [ $mo -lt $n_loops ]; then
cd $script_dir
qsub -v year=$year,var=$var,mo=$mo,time=$time run_save_10daily_quantities_10yr_loop.sh
fi

exit

