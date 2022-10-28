"""
Saves 10 daily quantities for one year of 025 JRA55 output (output given)
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
import climtas.nci

if __name__ == '__main__':

    climtas.nci.GadiClient()
    # Start a dask cluster with multiple cores
    #worker_dir=os.getenv('PBS_JOBFS')
    #if not worker_dir:
    #        worker_dir=os.getenv('TMPDIR')
    #if not worker_dir:
    #        worker_dir="/tmp"
    #client = Client(n_workers = 8, local_dir=worker_dir)
    #client = Client(n_workers=8, local_directory='/scratch/e14/cy8964/dask_dump/dask_worker_space')
    #### get run count argument that was passed to python script ####
    import sys
    year = int(sys.argv[1])
    run_count = int(sys.argv[2])
    run_count2 = int(sys.argv[3])
    ten_or_one_day = int(sys.argv[4])

    output = 'output'+str(year).zfill(3) +'/'

    # files:
    base = '/g/data/hh5/tmp/cy8964/access-om2/archive/025deg_jra55_ryf/'+output;
    xch = 1440*10#2*288
    ych = 1080*10#2*216

    fgrd   = xr.open_dataset(base + 'ocean/ocean_grid.nc').chunk({'yt_ocean':ych,'yu_ocean':ych,'xt_ocean':xch,'xu_ocean':xch})
    # load RYF daily data
    base_msc = '/g/data/hh5/tmp/cy8964/access-om2/archive/025deg_jra55_ryf/'+output
    # ---------------------------------------------------------------------------------- #
    # load in data set ----------------------------------------------------------------- #
    fdaily = xr.open_mfdataset(base_msc+'ocean/ocean_daily.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    fheat_daily = xr.open_mfdataset(base_msc+'ocean/ocean_heat_daily.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'yt_ocean':ych/10,'xt_ocean':xch/10})
    fwmass_daily = xr.open_mfdataset(base_msc+'ocean/ocean_wmass_daily.nc',combine='by_coords',chunks={'time': 1},decode_times = True).chunk({'time': 1,'grid_yt_ocean':ych/10,'grid_xt_ocean':xch/10})

    # ---------------------------------------------------------------------------------- #

    # Generate xgcm grid object:
    gridwm = Grid(fwmass_daily,coords={"x":{"center":"grid_xt_ocean"},
                                 "y":{"center":"grid_yt_ocean"},
                                 "T":{"center":"neutral","outer":"neutralrho_edges"}},periodic=False)
    gridmn = Grid(fdaily,coords={"x":{"center":"xt_ocean"},
                                 "y":{"center":"yt_ocean","right":"yu_ocean"},
                                 "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)
    gridht = Grid(fheat_daily,coords={"x":{"center":"xt_ocean"},
                                 "y":{"center":"yt_ocean"},
                                 "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)
    gridd = Grid(fdaily,coords={"x":{"center":"xt_ocean"},
                                 "y":{"center":"yt_ocean","right":"yu_ocean"},
                                 "z":{"center":"st_ocean","outer":"st_edges_ocean"}},periodic=False)

    # ---------------------------------------------------------------------------------- #

    # Some constants:
    Cp = 3992.10322329649
    rho0 = 1035
    dT = (fwmass_daily.neutral[1]-fwmass_daily.neutral[0]).values

    sc_daylength = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    area = fgrd.area_t.rename({'xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    dy = gridmn.diff(fgrd.yu_ocean,'y',boundary='extend').rename({'yt_ocean':'grid_yt_ocean'})

    def running_av_10days(variable):
        running_mean = (variable).roll(time = 1, roll_coords = False)+ (variable) + (variable).roll(time = 2, roll_coords = False)+ (variable).roll(time = 3, roll_coords = False)+(variable).roll(time = 4, roll_coords = False)+(variable).roll(time = 5, roll_coords = False)+(variable).roll(time = 6, roll_coords = False)+(variable).roll(time = 7, roll_coords = False)+(variable).roll(time = 8, roll_coords = False)+(variable).roll(time = 9, roll_coords = False)#.roll(time = -1, roll_coords = False)+2*(variable*month_length)
        running_mean = running_mean/10
        return running_mean

    obj, obj2 = xr.broadcast(fheat_daily.sfc_hflux_pme,fheat_daily.temp_vdiffuse_sbc)
    sfc_hflux_pme_daily = obj.where(fheat_daily.st_ocean==fheat_daily.st_ocean[0],other=0.)
    #fheat_daily["sfc_hflux_pme_daily"]=sfc_hflux_pme_daily
    # obj, obj2 = xr.broadcast(fdaily.pme_river,fheat_daily.temp_vdiffuse_sbc)
    # pme_river_daily = obj.where(fheat_daily.st_ocean==fheat_daily.st_ocean[0],other=0.)
    # fheat_daily["pme_river_daily"]=pme_river_daily

    MX_cv = fheat_daily.temp_vdiffuse_diff_cbt + \
                          fheat_daily.temp_nonlocal_KPP 
    SF_cv = fheat_daily.temp_vdiffuse_sbc + \
                          fheat_daily.sw_heat + \
                          fheat_daily.frazil_3d + \
                          sfc_hflux_pme_daily 


    # define bin edges
    tbins = fwmass_daily.neutralrho_edges.values

    # Calculate 10 day means
    #fwmass_daily['ty_trans_nrho_10day']= running_av_10days(fwmass_daily.ty_trans_nrho)
    temp_10day = running_av_10days(fdaily.temp)
    ty_trans_10day = running_av_10days(fdaily.ty_trans)

    # temperature binning of 10daily
    temp_u_10daily = gridd.interp(temp_10day,'y',boundary='extend')-273.15
    ty_trans_nrho_mean_10day = histogram(temp_u_10daily, bins=[tbins], dim = ['st_ocean'],weights=ty_trans_10day).rename({temp_u_10daily.name+'_bin':'neutral','xt_ocean':'grid_xt_ocean','yu_ocean':'grid_yu_ocean'})

    # temperature binning of 1daily
    temp_u_daily = gridd.interp(fdaily.temp,'y',boundary='extend')-273.15
    ty_trans_nrho_mean_1day = histogram(temp_u_daily, bins=[tbins], dim = ['st_ocean'],weights=fdaily.ty_trans).rename({temp_u_daily.name+'_bin':'neutral','xt_ocean':'grid_xt_ocean','yu_ocean':'grid_yu_ocean'})

    ##10 day means

    SF_cv_10day = running_av_10days(SF_cv)
    MX_cv_10day = running_av_10days(MX_cv)

    # fheat_daily['pme_river_10day'] = running_av_10days(fheat_daily.pme_river_daily)
    # fheat_daily['temp_rivermix_10day'] = running_av_10days(fheat_daily.temp_rivermix)

    # fheat_daily['sw_heat_10day'] = running_av_10days(fheat_daily.sw_heat)
    # fheat_daily['frazil_3d_10day'] = running_av_10days(fheat_daily.frazil_3d)
    # fheat_daily['temp_vdiffuse_sbc_10day'] = running_av_10days(fheat_daily.temp_vdiffuse_sbc)
    # fheat_daily['sfc_hflux_pme_10day'] = running_av_10days(fheat_daily.sfc_hflux_pme_daily)
    temp_nonlocal_KPP_10day = running_av_10days(fheat_daily.temp_nonlocal_KPP)
    temp_vdiffuse_diff_cbt_kppish_10day = running_av_10days(fheat_daily.temp_vdiffuse_diff_cbt_kppish)
    # fheat_daily['temp_vdiffuse_diff_cbt_kppicon_10day'] = running_av_10days(fheat_daily.temp_vdiffuse_diff_cbt_kppicon)
    temp_vdiffuse_diff_cbt_kppbl_10day = running_av_10days(fheat_daily.temp_vdiffuse_diff_cbt_kppbl)
    # fheat_daily['temp_vdiffuse_diff_cbt_kppdd_10day'] = running_av_10days(fheat_daily.temp_vdiffuse_diff_cbt_kppdd)
    # fheat_daily['temp_vdiffuse_diff_cbt_wave_10day'] = running_av_10days(fheat_daily.temp_vdiffuse_diff_cbt_wave)


    # temperature binning of daily and 10 daily data
    # SF and MX:
    SF_cv_mean_1day = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=SF_cv).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    MX_cv_mean_1day = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=MX_cv).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    SF_cv_mean_10day = histogram(temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=SF_cv_10day).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    MX_cv_mean_10day = histogram(temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=MX_cv_10day).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # SF decomposition
    # fwmass_daily["sw_heat_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.sw_heat).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["sw_heat_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.sw_heat_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["frazil_3d_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.frazil_3d).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["frazil_3d_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.frazil_3d_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["temp_vdiffuse_sbc_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_sbc).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["temp_vdiffuse_sbc_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_sbc_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["sfc_hflux_pme_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.sfc_hflux_pme_daily).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["sfc_hflux_pme_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.sfc_hflux_pme_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["temp_rivermix_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_rivermix).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["temp_rivermix_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_rivermix_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["pme_river_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.pme_river_daily).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["pme_river_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.pme_river_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # MX decomposition

    temp_nonlocal_KPP_mean_1day = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_nonlocal_KPP).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    temp_nonlocal_KPP_mean_10day = histogram(temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=temp_nonlocal_KPP_10day).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})


    temp_vdiffuse_diff_cbt_kppish_mean_1day = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppish).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    temp_vdiffuse_diff_cbt_kppish_mean_10day = histogram(temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=temp_vdiffuse_diff_cbt_kppish_10day).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["temp_vdiffuse_diff_cbt_kppicon_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppicon).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["temp_vdiffuse_diff_cbt_kppicon_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppicon_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    temp_vdiffuse_diff_cbt_kppbl_mean_1day = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppbl).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    temp_vdiffuse_diff_cbt_kppbl_mean_10day = histogram(temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=temp_vdiffuse_diff_cbt_kppbl_10day).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["temp_vdiffuse_diff_cbt_kppdd_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppdd).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["temp_vdiffuse_diff_cbt_kppdd_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_kppdd_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})

    # fwmass_daily["temp_vdiffuse_diff_cbt_wave_mean_1day"] = histogram(fdaily.temp-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_wave).rename({'temp_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})
    # fwmass_daily["temp_vdiffuse_diff_cbt_wave_mean_10day"] = histogram(fdaily.temp_10day-273.15, bins=[tbins], dim = ['st_ocean'],weights=fheat_daily.temp_vdiffuse_diff_cbt_wave_10day).rename({'temp_10day_bin':'neutral','xt_ocean':'grid_xt_ocean','yt_ocean':'grid_yt_ocean'})


    # # same for 1 day mean:
    SF_1day = -gridwm.cumsum((SF_cv_mean_1day*area), 'T',boundary="fill",fill_value=0)
    MX_1day = -gridwm.cumsum((MX_cv_mean_1day*area), 'T',boundary="fill",fill_value=0)

    # sw_heat_1day = -gridwm.cumsum((fwmass_daily.sw_heat_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # frazil_3d_mean_1day = -gridwm.cumsum((fwmass_daily.frazil_3d_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_sbc_mean_1day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_sbc_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # sfc_hflux_pme_mean_1day  = -gridwm.cumsum((fwmass_daily.sfc_hflux_pme_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # pme_river_mean_1day  = -gridwm.cumsum((fwmass_daily.pme_river_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # temp_rivermix_mean_1day  = -gridwm.cumsum((fwmass_daily.temp_rivermix_mean_1day*area), 'T',boundary="fill",fill_value=0)
    temp_nonlocal_KPP_mean_1day  = -gridwm.cumsum((temp_nonlocal_KPP_mean_1day*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish_1day = -gridwm.cumsum((temp_vdiffuse_diff_cbt_kppish_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_kppicon_1day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_kppicon_mean_1day*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl_1day = -gridwm.cumsum((temp_vdiffuse_diff_cbt_kppbl_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_kppdd_1day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_kppdd_mean_1day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_wave_1day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_wave_mean_1day*area), 'T',boundary="fill",fill_value=0)


    #same for 10 day mean:
    SF_10day = -gridwm.cumsum((SF_cv_mean_10day*area), 'T',boundary="fill",fill_value=0)
    MX_10day = -gridwm.cumsum((MX_cv_mean_10day*area), 'T',boundary="fill",fill_value=0)

    # sw_heat_10day = -gridwm.cumsum((fwmass_daily.sw_heat_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # frazil_3d_mean_10day = -gridwm.cumsum((fwmass_daily.frazil_3d_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_sbc_mean_10day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_sbc_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # sfc_hflux_pme_mean_10day  = -gridwm.cumsum((fwmass_daily.sfc_hflux_pme_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # pme_river_mean_10day  = -gridwm.cumsum((fwmass_daily.pme_river_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # temp_rivermix_mean_10day  = -gridwm.cumsum((fwmass_daily.temp_rivermix_mean_10day*area), 'T',boundary="fill",fill_value=0)
    temp_nonlocal_KPP_mean_10day  = -gridwm.cumsum((temp_nonlocal_KPP_mean_10day*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppish_10day = -gridwm.cumsum((temp_vdiffuse_diff_cbt_kppish_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_kppicon_10day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_kppicon_mean_10day*area), 'T',boundary="fill",fill_value=0)
    temp_vdiffuse_diff_cbt_kppbl_10day = -gridwm.cumsum((temp_vdiffuse_diff_cbt_kppbl_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_kppdd_10day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_kppdd_mean_10day*area), 'T',boundary="fill",fill_value=0)
    # temp_vdiffuse_diff_cbt_wave_10day = -gridwm.cumsum((fwmass_daily.temp_vdiffuse_diff_cbt_wave_mean_10day*area), 'T',boundary="fill",fill_value=0)

    # Calculate stream function for 1 yr mean transport 

    Psi_mean_10day = gridwm.cumsum(ty_trans_nrho_mean_10day, 'T',boundary="fill", fill_value=0)/rho0

    Psi_mean_1day = gridwm.cumsum(ty_trans_nrho_mean_1day, 'T',boundary="fill", fill_value=0)/rho0
    
    ## COMPUTE FOR 10 or 1 DAYS
    j = run_count ## choice of variable
    k = run_count2 ## choice of month

    if ten_or_one_day ==10:

        daily_array = [Psi_mean_10day, SF_10day, MX_10day, \
                       temp_nonlocal_KPP_mean_10day, temp_vdiffuse_diff_cbt_kppish_10day,\
                       temp_vdiffuse_diff_cbt_kppbl_10day][j]
        daily_array_name = ['Psi_mean_10day', 'SF_10day', 'MX_10day', \
                            'temp_nonlocal_KPP_10day','temp_vdiffuse_diff_cbt_kppish_10day', \
                            'temp_vdiffuse_diff_cbt_kppbl_10day'][j]

        if k ==11:
            daily_array = daily_array.isel(time = np.arange(k*30,k*30+35))
        else:
            daily_array = daily_array.isel(time = np.arange(k*30,k*30+30))
        daily_array_timemean = daily_array.sum('time')
        nt = len(daily_array.time.values)
        daily_array_timemean = daily_array_timemean/nt
        daily_array_timemean.load()
        ds = xr.Dataset({daily_array_name: daily_array_timemean})
        ds.to_netcdf('/g/data/e14/cy8964/Post_Process/10_year_daily_data/'+output+'10daily_mean_1year_means_'+daily_array_name+'_mo'+str(k)+'.nc', 
                     encoding={daily_array_name: {'shuffle': True, 'zlib': True, 'complevel': 5}})
    elif ten_or_one_day ==1:

        daily_array = [Psi_mean_1day, SF_1day, MX_1day, \
                       temp_nonlocal_KPP_mean_1day, temp_vdiffuse_diff_cbt_kppish_1day,\
                       temp_vdiffuse_diff_cbt_kppbl_1day][j]
        daily_array_name = ['Psi_mean_1day', 'SF_1day', 'MX_1day', \
                            'temp_nonlocal_KPP_1day','temp_vdiffuse_diff_cbt_kppish_1day', \
                            'temp_vdiffuse_diff_cbt_kppbl_1day'][j]

        k = run_count2
        if k ==11:
            daily_array = daily_array.isel(time = np.arange(k*30,k*30+35))
        else:
            daily_array = daily_array.isel(time = np.arange(k*30,k*30+30))
        daily_array_timemean = daily_array.sum('time')
        nt = len(daily_array.time.values)
        daily_array_timemean = daily_array_timemean/nt
        daily_array_timemean.load()
        ds = xr.Dataset({daily_array_name: daily_array_timemean})
        ds.to_netcdf('/g/data/e14/cy8964/Post_Process/10_year_daily_data/'+output+'1daily_mean_1year_means_'+daily_array_name+'_mo'+str(k)+'.nc', 
                     encoding={daily_array_name: {'shuffle': True, 'zlib': True, 'complevel': 5}})
    else: 
        print('Help, timescale not found')
