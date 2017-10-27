from math import sqrt, pi
import os

params = {}
homedir = os.environ['HOME']

params['debug'] = False
params['verbosity'] = '1'

# Do we run the model as a job array with each sector as a job ?
# If yes, we need to adapt the paths to files accordingly

params['array'] = False

if params['array']:
    tmpdir = os.environ['MYTMPDIR']

params['remove_tmps'] = True
params['remove_old_results_file'] = True

params['processes'] = 4

params['constraint_calculation_threshold'] = 0.01
params['constraint_calculation_max_iter'] = 50

# This defines the file(s) containing the list of sectors to be treated
if params['array']:
	params['nace_codes_file'] = tmpdir + '/GRAVITY_IO/SECTORLISTS/sectors_2010_%s'
else:
	#params['nace_codes_file'] = homedir + '/GRAVITY_IO/SECTORLISTS/sectors.csv'
	params['nace_codes_file'] = 'sectors_one.csv'

# Either we use individual firms, or we sum 
# the volumes of all firms in a sector within 
# one pixel.
# In the former case, 'pointmap' points to the vector map
# containing all individual firms.
# In the latter case, 'pointmap' points to 
# a basename prefix to which the NACE codes are added
# Each resulting map has one vector point per pixel
# with the necessary attributes (x, y, volume, spatial unit)
params['firms_grouped_by_pixel'] = False
params['pointmap'] = 'produnits_2010'

# Columns of the point map ('x', 'y' are also supposed to be present) 
# spatial_unit_name corresponds to the spatial_unit_code below.
# For each firm, we thus need an already defined columns containing the code of
# the spatial unit it belongs to.
params['nace_info_column'] = 'cd_nace_2010'
params['spatial_unit_name'] = 'arr'
params['volume_measure'] = 'turnover_estim'

# Name of the map containing the spatial units in which we aggregate the results
params['spatial_unit_map'] = 'arrondissements_be'
# Column with spatial unit codes in the vector attributes
# of the spatial unit map. Must be integer.
params['spatial_unit_code'] = 'unit_code'
# name of GRASS raster map with population per pixel or prefix of per NACE
# population / consumption map
params['destination_map'] = 'consumption_2010_'
params['use_per_nace_pop'] = True
params['dest_map_nace_precision'] = 2

# File with info on exports and investment shares per nace
# and (zero-based) columns numbers in which to find the info.
# This can be the same file needed for the calculation of the consumption
# populations.
if params['array']:
    params['nace_export_investment_shares_file'] = tmpdir + '/GRAVITY_IO/AUX/io_shares.csv'
else:
    params['nace_export_investment_shares_file'] = 'AUX/io_shares.csv'
params['nace_col'] = 0
params['investment_col'] = 66
params['export_col'] = 67

# At which sector code resolution (i.e. how many digits) do we have the information 
# about exports and investements in the IO-table ?
params['exp_inv_nace_precision'] = 2


# Which map should we use as mask and at which resolution should the model be
# calculated ?
params['mask_vector'] = 'belgique'
params['resolution'] = 1000

# Determine the distance to artifically assign to firms that are located within 
# the current cell, so for which the calculated distance is somewhat arbitrary. 
# We take the rayon of a circle with an area equal to half of the area of a grid
# cell.
params['internal_distance'] = sqrt ( ( params['resolution'] ** 2 / 2.0 ) / pi ) 

# Output file
if params['array']:
    if params['processes'] > 1:
        params['trade_matrix_employment_file'] = homedir + '/MODELRESULTS/trade_matrix_%s_%s_%s_%s.csv' % (params['spatial_unit_name'], params['pointmap'], params['volume_measure'], '%s')
    else:
        params['trade_matrix_employment_file'] = homedir + '/MODELRESULTS/trade_matrix_%s_%s_%s_%s_%s.csv' % (params['spatial_unit_name'], params['pointmap'], params['volume_measure'], '%s', '%s')
else:
    if params['processes'] > 1:
        params['trade_matrix_employment_file'] = 'results_test_%s.csv' % '%s'
    else:
        params['trade_matrix_employment_file'] = 'results_newmodel_2010.csv'

# File containing criterium for gamma calculation
# Generally, this will be a file resulting from running the bash script
# 'calculate_distances_to_all_pixels.grass'
if params['array']:
    params['gamma_file'] = tmpdir + '/GRAVITY_IO/AUX/DISTANCES_2010/weighted_mean.csv'
    params['nace_gamma_file'] = homedir + '/MODELOUTPUTS/nace_gamma_file_2010.csv'
else:
    params['gamma_file'] = 'AUX/DISTANCES_2010/weighted_mean.csv'
    params['nace_gamma_file'] = False

# Is there a header line in the gamma_file ?
params['headerline'] = True
params['gammadir'] = -1 # 1: direction of criterium = direction of gamma, else -1

# Min and Max of the possible gamma values
# Unless some empirical evaluation exists this is somewhat arbitrary.
# As the international trade literature finds values close to 1 we let the value
# vary between 0 and 2.
params['min_gamma'] = 0.0
params['max_gamma'] = 2.0

# min percentile below which all values should be equal to min_gamma
params['min_perc'] = 0.10
# max percentile above which all values should be equal to max_gamma
params['max_perc'] = 0.90

# percentile of volume to use as threshold for export estimation
# only firms with a volume above this percentile will be considered as exporting
# firms
params['volume_perc'] = 0.5
