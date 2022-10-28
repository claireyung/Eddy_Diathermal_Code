"""
Saves 2mo smoothed quantities for 10 years of monthly data using xhistogram - 3d and extra SF terms
"""

# Load modules

# Standard modules
import matplotlib.pyplot as plt
import xarray as xr
from xgcm import Grid
import numpy as np
import pandas as pd
import cftime

import IPython.display
import cmocean as cm
import cartopy.crs as ccrs
import cartopy.feature as cft
import sys, os
import warnings
warnings.simplefilter("ignore")
from dask.distributed import Client

from xhistogram.xarray import histogram


if __name__ == '__main__':

    # Start a dask cluster with multiple cores
    client = Client(n_workers=8, local_directory='/scratch/e14/cy8964/dask_dump/dask_worker_space')
    #### get run count argument that was passed to python script ####
    import sys
    run_count = int(sys.argv[1])
    run_count2 = int(sys.argv[2])
    

    # files:
    base = '/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/';
    xch = 1440#2*288
    ych = 1080#2*216

    fgrd   = xr.open_dataset(base + 'output096/ocean/ocean_grid.nc').chunk({'yt_ocean':ych,'yu_ocean':ych,'xt_ocean':xch,'xu_ocean':xch})
    fwmass_mean = xr.open_mfdataset('/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/output096-105_mean/ocean_wmass_mean.nc').chunk({'grid_yt_ocean':ych,'grid_yu_ocean':ych,'grid_xt_ocean':xch})
    fmonth_mean = xr.open_mfdataset('/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/output096-105_mean/ocean_month_mean.nc').chunk({'yt_ocean':ych,'yu_ocean':ych,'xt_ocean':xch})
    fheat_mean = xr.open_mfdataset('/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/output096-105_mean/ocean_heat_mean.nc').chunk({'yt_ocean':ych,'xt_ocean':xch})

    # load RYF sc data
    base_msc = '/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/output096-105_mean/'
    # ---------------------------------------------------------------------------------- #
    # load in data set ----------------------------------------------------------------- #
    fmonth_sc = xr.open_mfdataset(base_msc+'ocean_month.ncea.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    fheat_sc = xr.open_mfdataset(base_msc+'ocean_heat.ncea.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    fwmass_sc = xr.open_mfdataset(base_msc+'ocean_wmass.ncea.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'grid_yt_ocean':ych/10,'grid_xt_ocean':xch/10})
    fwmass_mix_sc = xr.open_mfdataset(base_msc+'ocean_wmass_diff_cbt.ncea.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'grid_yt_ocean':ych/10,'grid_xt_ocean':xch/10})

    #### load output of monthly data for 8 years (RYF) #### output 086-094
    base_msc = '/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/'
    # ---------------------------------------------------------------------------------- #
    nr_outputs = 10    # define number of output files to consider
    last_output = 105  # the last output to consider loading in
    # ---------------------------------------------------------------------------------- #
    a = [i for i in range(last_output+1-nr_outputs,last_output+1)] # create integer list
    s = list(range(0,nr_outputs))
    c = [] # empty list which I fill up
    d = []
    e = []
    for i in s: # loop through the number of files I would like
        #c.append(i)
        d.append(i)
        e.append(i)
        # fill in empty list with integers trailing two zeros (000, 001, 002, ...)
        #c[i] = base_msc+'output'+str(a[i]).zfill(3) + '/ocean/ocean_wmass.nc' 
        d[i] = base_msc+'output'+str(a[i]).zfill(3) + '/ocean/ocean_heat.nc' 
        e[i] = base_msc+'output'+str(a[i]).zfill(3) + '/ocean/ocean_month.nc' 

    # load in data set ----------------------------------------------------------------- #
    #fwmass_m = xr.open_mfdataset(c,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'grid_yt_ocean':ych/10,'grid_yu_ocean':ych/10,'grid_xt_ocean':xch/10})
    fheat_m = xr.open_mfdataset(d,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    fmonth_m = xr.open_mfdataset(e,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'yu_ocean':ych/10,'xt_ocean':xch/10})


    #### load output of monthly data for 8 years (RYF) #### output 086-094
    base_msc = '/scratch/e14/cy8964/access-om2/archive/025deg_jra55_ryf/'
    # ---------------------------------------------------------------------------------- #
    nr_outputs = 1    # define number of output files to consider
    last_output = 105  # the last output to consider loading in
    # ---------------------------------------------------------------------------------- #
    a = [i for i in range(last_output+1-nr_outputs,last_output+1)] # create integer list
    s = list(range(0,nr_outputs))
    c = [] # empty list which I fill up
    d = []
    e = []
    for i in s: # loop through the number of files I would like
        c.append(i)

        # fill in empty list with integers trailing two zeros (000, 001, 002, ...)
        c[i] = base_msc+'output'+str(a[i]).zfill(3) + '/ocean/ocean_wmass.nc' 

    # load in data set ----------------------------------------------------------------- #
    fwmass_m1 = xr.open_mfdataset(c,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'grid_yt_ocean':ych/10,'grid_yu_ocean':ych/10,'grid_xt_ocean':xch/10})
    #fheat_m = xr.open_mfdataset(d,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    #fmonth_m = xr.open_mfdataset(e,combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'yu_ocean':ych/10,'xt_ocean':xch/10})

    # ---------------------------------------------------------------------------------- #

    # Generate xgcm grid object:
    gridwm = Grid(fwmass_m1,coords={"x":{"center":"grid_xt_ocean"},
                                 "y":{"center":"grid_yt_ocean","right":"grid_yu_ocean"},
                                 "T":{"center":"neutral","outer":"neutralrho_edges"}},periodic=False)
    gridmn = Grid(fmonth_m,coords={"x":{"center":"xt_ocean"},
                                 "y":{"center":"yt_ocean","right":"yu_ocean"},
                                 "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)
    gridht = Grid(fheat_m,coords={"x":{"center":"xt_ocean"},
                                 "y":{"center":"yt_ocean"},
                                 "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)
    # gridd = Grid(fdaily,coords={"x":{"center":"xt_ocean"},
    #                              "y":{"center":"yt_ocean","right":"yu_ocean"},
    #                              "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)

    # ---------------------------------------------------------------------------------- #

    # Some constants:
    Cp = 3992.10322329649
    rho0 = 1035
    dT = (fwmass_m1.neutral[1]-fwmass_m1.neutral[0]).values

    sc_daylength = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    area = fgrd.area_t.rename({'xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    dy = gridmn.diff(fgrd.yu_ocean,'y',boundary='extend').rename({'yt_ocean':'grid_yt_ocean'})

    def running_av_2mos(variable):
        month_length = variable.time.dt.days_in_month
        running_mean = (variable*month_length).roll(time = 1, roll_coords = False)+ (variable*month_length)#.roll(time = -1, roll_coords = False)+2*(variable*month_length)
        time_length = (month_length).roll(time = 1, roll_coords = False)+ (month_length)#.roll(time = -1, roll_coords = False)+2*(month_length)
        running_mean = running_mean/time_length
        return running_mean

    def weighted_time_mean_2mo_filter(variable):
        month_length = running_av_2mos(variable.time.dt.days_in_month)
        time_mean = (variable*month_length).sum('time')/(month_length.sum('time'))
        return time_mean

    def weighted_time_mean_2mo_filter_mo(variable, variable1):
        month_length = running_av_2mos(variable1.time.dt.days_in_month).groupby('time.month').mean('time')
        time_mean = (variable*month_length).sum('month')/(month_length.sum('month'))
        return time_mean

    days_in_month_sc = fheat_sc.average_DT/3600/24/1e9
    days_in_month_sc

    def weighted_time_mean(variable,days_in_month_sc):
        time_mean = (variable*days_in_month_sc).sum('time')/(days_in_month_sc.sum('time'))
        return time_mean

    fwmass_m = fheat_m.copy()
    # # Group SF and MX terms for monthly data:
    # fwmass_m["SF_cv"] = fwmass_m.temp_vdiffuse_sbc_on_nrho + \
    #                        fwmass_m.sfc_hflux_pme_on_nrho + \
    #                        fwmass_m.frazil_on_nrho + \
    #                        fwmass_m.sw_heat_on_nrho# + \
    # #                       fwmass_m.temp_rivermix_on_nrho
    # fwmass_m["MX_cv"] = fwmass_m.temp_vdiffuse_diff_cbt_on_nrho + \
    #                        fwmass_m.temp_nonlocal_KPP_on_nrho 

    obj, obj2 = xr.broadcast(fheat_m.sfc_hflux_pme,fheat_m.temp_vdiffuse_sbc)
    sfc_hflux_pme_m = obj.where(fheat_m.st_ocean==fheat_m.st_ocean[0],other=0.)

    fheat_m["SF_cv"] = fheat_m.temp_vdiffuse_sbc + \
                          sfc_hflux_pme_m + \
                          fheat_m.frazil_3d + \
                          fheat_m.sw_heat# + \
    #                      fheat_m.temp_rivermix
    fheat_m["MX_cv"] = fheat_m.temp_vdiffuse_diff_cbt + \
                          fheat_m.temp_nonlocal_KPP 
    # Group SF and MX terms for sc data:

    obj, obj2 = xr.broadcast(fheat_sc.sfc_hflux_pme,fheat_sc.temp_vdiffuse_sbc)
    sfc_hflux_pme_sc = obj.where(fheat_sc.st_ocean==fheat_sc.st_ocean[0],other=0.)

    fheat_sc["MX_cv"] = fheat_sc.temp_vdiffuse_diff_cbt + \
                          fheat_sc.temp_nonlocal_KPP 
    fheat_sc["SF_cv"] = fheat_sc.temp_vdiffuse_sbc + \
                          fheat_sc.sw_heat + \
                          fheat_sc.frazil_3d + \
                          sfc_hflux_pme_sc 
    #                      fheat_sc.temp_rivermix
    fwmass_sc["MX_cv"] = fwmass_sc.temp_vdiffuse_diff_cbt_on_nrho + \
                          fwmass_sc.temp_nonlocal_KPP_on_nrho 
    fwmass_sc["SF_cv"] = fwmass_sc.temp_vdiffuse_sbc_on_nrho + \
                          fwmass_sc.sw_heat_on_nrho + \
                          fwmass_sc.sfc_hflux_pme_on_nrho + \
                          fwmass_sc.frazil_on_nrho
    # Group mean SF and MX terms:
    fwmass_mean["SF_cv"] = fwmass_mean.temp_vdiffuse_sbc_on_nrho + \
                           fwmass_mean.sfc_hflux_pme_on_nrho + \
                           fwmass_mean.frazil_on_nrho + \
                           fwmass_mean.sw_heat_on_nrho #+ \
                           #fwmass_mean.temp_rivermix_on_nrho
    fwmass_mean["MX_cv"] = fwmass_mean.temp_vdiffuse_diff_cbt_on_nrho + \
                           fwmass_mean.temp_nonlocal_KPP_on_nrho 

    obj, obj2 = xr.broadcast(fheat_mean.sfc_hflux_pme,fheat_mean.temp_vdiffuse_sbc)
    sfc_hflux_pme_mean = obj.where(fheat_mean.st_ocean==fheat_mean.st_ocean[0],other=0.)

    fheat_mean["SF_cv"] = fheat_mean.temp_vdiffuse_sbc + \
                          sfc_hflux_pme_mean + \
                          fheat_mean.frazil_3d + \
                          fheat_mean.sw_heat # + \
    #                      fheat_mean.temp_rivermix
    fheat_mean["MX_cv"] = fheat_mean.temp_vdiffuse_diff_cbt + \
                          fheat_mean.temp_nonlocal_KPP


    tbins = fwmass_m1.neutralrho_edges.values

    # calculate a running mean of 2 months of ty_trans_nrho, temp and ty_trans and add them back into datasets
    fmonth_m['temp_2mo'] = running_av_2mos(fmonth_m.temp)
    fmonth_m['ty_trans_2mo'] = running_av_2mos(fmonth_m.ty_trans)

    # temperature binning of 2 month running average of both temp and ty_trans:
    temp_u_mean = gridmn.interp(fmonth_m.temp_2mo,'y',boundary='extend')-273.15
    fwmass_m["ty_trans_nrho_mean_2mo"] = histogram(temp_u_mean, bins=[tbins], dim = ['st_ocean'],weights=fmonth_m.ty_trans_2mo).rename({temp_u_mean.name+'_bin':'neutral','xt_ocean':'grid_xt_ocean','yu_ocean':'grid_yu_ocean'})

    # temperature binning of 10 year
    temp_u_mean_tot = gridmn.interp(fmonth_mean.temp,'y',boundary='extend')-273.15
    fwmass_mean["ty_trans_nrho_mean_1yr"] = histogram(temp_u_mean_tot, bins=[tbins], dim = ['st_ocean'],weights=fmonth_mean.ty_trans).rename({temp_u_mean_tot.name+'_bin':'neutral','xt_ocean':'grid_xt_ocean','yu_ocean':'grid_yu_ocean'})

    # calculate a running mean of 2 months of seasonal climatology
    fwmass_sc['ty_trans_nrho_2mo'] = running_av_2mos(fwmass_sc.ty_trans_nrho)
    fmonth_sc['temp_2mo'] = running_av_2mos(fmonth_sc.temp)
    fmonth_sc['ty_trans_2mo'] = running_av_2mos(fmonth_sc.ty_trans)

    # temperature binning of 2 month running average of both temp and ty_trans:
    temp_u_mean_sc = gridmn.interp(fmonth_sc.temp_2mo,'y',boundary='extend')-273.15
    fwmass_sc["ty_trans_nrho_mean_2mo"] = histogram(temp_u_mean_sc, bins=[tbins], dim = ['st_ocean'],weights=fmonth_sc.ty_trans_2mo).rename({temp_u_mean_sc.name+'_bin':'neutral','xt_ocean':'grid_xt_ocean','yu_ocean':'grid_yu_ocean'})

    # calculate a running mean of 2 months of things
    fheat_m['SF_cv_2mo'] = running_av_2mos(fheat_m.SF_cv)
    fheat_m['MX_cv_2mo'] = running_av_2mos(fheat_m.MX_cv)

    fheat_m['sw_heat_2mo'] = running_av_2mos(fheat_m.sw_heat)
    fheat_m['frazil_3d_2mo'] = running_av_2mos(fheat_m.frazil_3d)
    fheat_m['temp_vdiffuse_sbc_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_sbc)
    #sfc_hflux_pme by residual

    fheat_m['temp_vdiffuse_diff_cbt_kppish_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_diff_cbt_kppish)
    fheat_m['temp_vdiffuse_diff_cbt_kppicon_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_diff_cbt_kppicon)
    fheat_m['temp_vdiffuse_diff_cbt_kppbl_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_diff_cbt_kppbl)
    fheat_m['temp_vdiffuse_diff_cbt_kppdd_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_diff_cbt_kppdd)
    fheat_m['temp_vdiffuse_diff_cbt_wave_2mo'] = running_av_2mos(fheat_m.temp_vdiffuse_diff_cbt_wave)
    #temp_nonlocal_KPP by residual

    ##----##
    # same for seasonal climatology

    # calculate a running mean of 2 months of things
    fheat_sc['SF_cv_2mo'] = running_av_2mos(fheat_sc.SF_cv)
    fheat_sc['MX_cv_2mo'] = running_av_2mos(fheat_sc.MX_cv)

    fheat_sc['sw_heat_2mo'] = running_av_2mos(fheat_sc.sw_heat)
    fheat_sc['frazil_3d_2mo'] = running_av_2mos(fheat_sc.frazil_3d)
    fheat_sc['temp_vdiffuse_sbc_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_sbc)
    #sfc_hflux_pme by residual

    fheat_sc['temp_vdiffuse_diff_cbt_kppish_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_diff_cbt_kppish)
    fheat_sc['temp_vdiffuse_diff_cbt_kppicon_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_diff_cbt_kppicon)
    fheat_sc['temp_vdiffuse_diff_cbt_kppbl_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_diff_cbt_kppbl)
    fheat_sc['temp_vdiffuse_diff_cbt_kppdd_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_diff_cbt_kppdd)
    fheat_sc['temp_vdiffuse_diff_cbt_wave_2mo'] = running_av_2mos(fheat_sc.temp_vdiffuse_diff_cbt_wave)
    #temp_nonlocal_KPP by residual

    # temperature binning of 2 month mean
    # SF and MX:
    #temp_u_mean = gridmn.interp(fmonth_m.temp_2mo,'y',boundary='extend')-273.15
    fwmass_m["SF_cv_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.SF_cv_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["MX_cv_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.MX_cv_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["sw_heat_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.sw_heat_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["frazil_3d_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.frazil_3d_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_sbc_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_sbc_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_diff_cbt_kppish_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_diff_cbt_kppish_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_diff_cbt_kppicon_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_diff_cbt_kppicon_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_diff_cbt_kppbl_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_diff_cbt_kppbl_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_diff_cbt_kppdd_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_diff_cbt_kppdd_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_m["temp_vdiffuse_diff_cbt_wave_mean_2mo"] = histogram(fmonth_m.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_m.temp_vdiffuse_diff_cbt_wave_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # temperature binning of 2 month mean
    # SF and MX:
    #temp_u_mean = gridmn.interp(fmonth_m.temp_2mo,'y',boundary='extend')-273.15
    fwmass_sc["SF_cv_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.SF_cv_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["MX_cv_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.MX_cv_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["sw_heat_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.sw_heat_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["frazil_3d_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.frazil_3d_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_sbc_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_sbc_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_diff_cbt_kppish_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_diff_cbt_kppish_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_diff_cbt_kppicon_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_diff_cbt_kppicon_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_diff_cbt_kppbl_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_diff_cbt_kppbl_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_diff_cbt_kppdd_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_diff_cbt_kppdd_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_sc["temp_vdiffuse_diff_cbt_wave_mean_2mo"] = histogram(fmonth_sc.temp_2mo-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_sc.temp_vdiffuse_diff_cbt_wave_2mo).rename({'temp_2mo_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # SF and MX: (use xhistogram because seems to have issues with xgcm)
    tbins = fwmass_m1.neutralrho_edges.values
    # apply histogram binning to st_ocean dimension
    fwmass_mean["SF_cv_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.SF_cv).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["MX_cv_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.MX_cv).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["sw_heat_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.sw_heat).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["frazil_3d_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.frazil_3d).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_sbc_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_sbc).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_diff_cbt_kppish_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_diff_cbt_kppish).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_diff_cbt_kppicon_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_diff_cbt_kppicon).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_diff_cbt_kppbl_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_diff_cbt_kppbl).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_diff_cbt_kppdd_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_diff_cbt_kppdd).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    fwmass_mean["temp_vdiffuse_diff_cbt_wave_mean_tot"] = histogram(fmonth_mean.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_mean.temp_vdiffuse_diff_cbt_wave).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})


    # True surface forcing and vertical mixing:
    SF = -gridwm.cumsum((fwmass_mean.SF_cv*area), 'T',boundary="fill",fill_value=0)
    MX = -gridwm.cumsum((fwmass_mean.MX_cv*area), 'T',boundary="fill",fill_value=0)

    sw_heat = -gridwm.cumsum((fwmass_mean.sw_heat_on_nrho*area), 'T',boundary="fill",fill_value=0)
    frazil_3d = -gridwm.cumsum((fwmass_mean.frazil_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_sbc = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_sbc_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppish_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppicon = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppicon_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppbl_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppdd = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppdd_on_nrho*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_wave = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_wave_on_nrho*area), 'T',boundary="fill",fill_value=0)

    # Same for 2 mo time mean:
    SF_mean_2mo = -gridwm.cumsum((fwmass_m.SF_cv_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    MX_mean_2mo = -gridwm.cumsum((fwmass_m.MX_cv_mean_2mo*area), 'T',boundary="fill",fill_value=0)

    sw_heat_mean_2mo = -gridwm.cumsum((fwmass_m.sw_heat_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    frazil_3d_mean_2mo = -gridwm.cumsum((fwmass_m.frazil_3d_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_sbc_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_sbc_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_diff_cbt_kppish_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppicon_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_diff_cbt_kppicon_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_diff_cbt_kppbl_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppdd_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_diff_cbt_kppdd_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_wave_mean_2mo = -gridwm.cumsum((fwmass_m.temp_vdiffuse_diff_cbt_wave_mean_2mo*area), 'T',boundary="fill",fill_value=0)

    # # Same for seasonal climatology:
    SF_mean_sc = -gridwm.cumsum((fwmass_sc.SF_cv_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    MX_mean_sc = -gridwm.cumsum((fwmass_sc.MX_cv_mean_2mo*area), 'T',boundary="fill",fill_value=0)

    sw_heat_mean_sc = -gridwm.cumsum((fwmass_sc.sw_heat_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    frazil_3d_mean_sc = -gridwm.cumsum((fwmass_sc.frazil_3d_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_sbc_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_sbc_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_diff_cbt_kppish_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppicon_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_diff_cbt_kppicon_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_diff_cbt_kppbl_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppdd_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_diff_cbt_kppdd_mean_2mo*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_wave_mean_sc = -gridwm.cumsum((fwmass_sc.temp_vdiffuse_diff_cbt_wave_mean_2mo*area), 'T',boundary="fill",fill_value=0)


    # Same for total time mean
    SF_mean_tot = -gridwm.cumsum((fwmass_mean.SF_cv_mean_tot*area), 'T',boundary="fill",fill_value=0)
    MX_mean_tot = -gridwm.cumsum((fwmass_mean.MX_cv_mean_tot*area), 'T',boundary="fill",fill_value=0)

    sw_heat_mean_tot = -gridwm.cumsum((fwmass_mean.sw_heat_mean_tot*area), 'T',boundary="fill",fill_value=0)
    frazil_3d_mean_tot = -gridwm.cumsum((fwmass_mean.frazil_3d_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_sbc_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_sbc_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppish_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppicon_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppicon_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppbl_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppdd_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_kppdd_mean_tot*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_wave_mean_tot = -gridwm.cumsum((fwmass_mean.temp_vdiffuse_diff_cbt_wave_mean_tot*area), 'T',boundary="fill",fill_value=0)


    # Calculate stream function for 1 yr mean transport 
    Psi = gridwm.cumsum(fwmass_mean.ty_trans_nrho, 'T',boundary="fill", fill_value=0)/rho0

    Psi_mean_tot = gridwm.cumsum(fwmass_mean.ty_trans_nrho_mean_1yr, 'T',boundary="fill", fill_value=0)/rho0
    Psi_mean_2mo = gridwm.cumsum(fwmass_m.ty_trans_nrho_mean_2mo, 'T',boundary="fill", fill_value=0)/rho0
    Psi_mean_sc = gridwm.cumsum(fwmass_sc.ty_trans_nrho_mean_2mo, 'T',boundary="fill", fill_value=0)/rho0

    ## j is the variable
    j = run_count
    ## k is the year
    k = run_count2
    month_lengths = (sc_daylength + np.roll(sc_daylength,1))/2
    month_lengths


    daily_array = [Psi_mean_2mo, SF_mean_2mo, MX_mean_2mo, sw_heat_mean_2mo, frazil_3d_mean_2mo, \
                   temp_vdiffuse_sbc_mean_2mo, temp_vdiffuse_diff_cbt_kppish_mean_2mo , \
                   temp_vdiffuse_diff_cbt_kppicon_mean_2mo, temp_vdiffuse_diff_cbt_kppbl_mean_2mo, \
                   temp_vdiffuse_diff_cbt_kppdd_mean_2mo, temp_vdiffuse_diff_cbt_wave_mean_2mo][j]
    daily_array_name = ['Psi_mean_2mo', 'SF_2mo', 'MX_2mo', 'sw_heat_2mo', 'frazil_3d_2mo', \
                        'temp_vdiffuse_sbc_2mo','temp_vdiffuse_diff_cbt_kppish_2mo', \
                        'temp_vdiffuse_diff_cbt_kppicon_2mo', 'temp_vdiffuse_diff_cbt_kppbl_2mo', \
                        'temp_vdiffuse_diff_cbt_kppdd_2mo', 'temp_vdiffuse_diff_cbt_wave_2mo'][j]
    daily_array = daily_array.isel(time = np.arange(k*12,k*12+12))
    daily_array_timemean = xr.zeros_like(daily_array.isel(time=0))
    nt = len(daily_array.time.values)
    for i in range(nt):
        print('Doing time step ' + str(i+1) + ' of ' + str(nt))
        daily_array_t = daily_array.isel(time=i);
        daily_array_t.load()
        daily_array_timemean += daily_array_t*month_lengths[i]
    daily_array_timemean = daily_array_timemean/365
    daily_array_timemean.load()
    ds = xr.Dataset({daily_array_name: daily_array_timemean})
    ds.to_netcdf('/scratch/e14/cy8964/temp/025deg_10yr_3d/2mo_mean_10year_means_'+daily_array_name+'_yr'+str(k)+'.nc', 
                 encoding={daily_array_name: {'shuffle': True, 'zlib': True, 'complevel': 5}})
