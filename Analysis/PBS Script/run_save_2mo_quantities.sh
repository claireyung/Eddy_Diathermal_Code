#!/bin/bash
#PBS -P e14
#PBS -q normalbw
#PBS -l mem=250gb,ncpus=16,walltime=0:30:00,storage=gdata/ik11+gdata/hh5+gdata/x77+gdata/e14+scratch/e14
#PBS -N save_binned_heat_terms_10yr_2mo
#PBS -v count,count2

module load conda

cd  /g/data/e14/cy8964/analysis/scripts/

echo "submitting term $count to gadi"

# call python
python save_2mo_quantities_xhistogram.py $count $count2 &>> output_save_2mo_quantities_xhistogram_count${count}_${count2}.txt
