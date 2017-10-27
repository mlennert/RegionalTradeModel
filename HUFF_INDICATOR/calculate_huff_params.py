params = {}

params['remove_tmps'] = True
params['remove_old_results_file'] = True
params['calc_consumption_pop_indicator'] = True
params['verbosity'] = '1'

params['processes'] = 4

params['pointmap'] = 'produnits'
params['nace_info_column'] = 'cd_nacebel_2010'
params['spatial_unit_name'] = 'arr'
params['spatial_unit_map'] = 'arrondissements_be'
params['spatial_unit_name_map'] = 'unit'
params['volume_measure'] = 'etab_emploi_onss_2010'

params['nace_codes_file'] = 'nace_codes.csv'

params['mask_vector'] = 'arrondissements_be'
params['resolution'] = 1000

params['pop_by_spatial_unit_and_nace_file'] = 'pop_by_%s_and_%s_%s.csv' % (params['spatial_unit_name'], params['pointmap'], params['volume_measure'])

if params['calc_consumption_pop_indicator']:
    params['empl_by_spatial_unit_and_nace_file'] = 'empl_by_%s_and_%s_%s.csv' % (params['spatial_unit_name'], params['pointmap'], params['volume_measure'])

# file containing criterium for gamma calculation
params['gamma_file'] = 'DISTANCES/perc90_produnits.csv'
# Is there a header line in the gamma_file ?
params['headerline'] = True
params['gammadir'] = -1 # 1: direction of criterium = direction of gamma, else -1
# Min and Max of the possible gamma values
params['min_gamma'] = 0.0
params['max_gamma'] = 2.0

# min percentile below which all values should be equal to min_gamma
params['min_perc'] = 0.0
# max percentile above which all values should be equal to max_gamma
params['max_perc'] = 0.75

# name of GRASS raster map with population per pixel
params['population_map'] = 'population'
# name of GRASS raster map with  per pixel
params['income_map'] = 'rev_med_rel2010'
params['consumption_population_map'] = 'consumption_population'
params['inc_tb'] = False # True: use income and national trade balance
params['calculate_total_consumption_potentials'] = False

params['trade_balance_over_output_by_nace'] = {'01': 0.0348685065, '02': 0.0348685065, '03':
        0.0348685065, '05': 0.2157681444, '06': 0.2157681444, '07': 0.2157681444, '08':
        0.2157681444, '09': 0.2157681444, '10': 0.126823219, '11': 0.126823219,
        '12': 0.126823219, '13': 0.338714145, '14': 0.338714145, '15':
        0.338714145, '16': 0.1494245017, '17': 0.1494245017, '18': 0.0632228664,
        '19': -0.1385474952, '20': 0.3187360688, '21': 0.4583422936, '22':
        0.2201900547, '23': 0.2062415949, '24': 0.0289517936, '25':
        0.0289517936, '26': 0.289675283, '27': 0.2586091154, '28': 0.2586091154,
        '29': 0.1869633838, '30': 0.1024331056, '31': 0.1636795566, '32':
        0.1636795566, '33': 0.0524146054, '35': -0.2842459619, '36':
        0.0532551408, '37': 0.0532551408, '38': 0.0532551408, '39':
        0.0532551408, '41': -0.0113885058, '42': -0.0113885058, '43':
        -0.0113885058, '45': -0.7614666888, '46': -0.3751247469, '47':
        -0.2866822553, '49': 0.0704970802, '50': 0.1973293139, '51':
        -0.2785301123, '52': 0.2006849484, '53': -0.0740468245, '55':
        0.489081529, '56': 0.489081529, '58': -0.0507392076, '59':
        -0.0253945908, '60': -0.0253945908, '61': -0.0408155731, '62':
        0.0523379591, '63': 0.0523379591, '64': 0.0887366619, '65':
        0.0887366619, '66': 0.0887366619, '68': -0.0009643594, '69':
        0.0776378043, '70': 0.0776378043, '71': 0.0239709418, '72':
        0.1746607394, '73': 0.0296845778, '74': 0.0035906643, '75':
        0.0035906643, '77': 0.0372791905, '78': 0.0106696169, '79':
        0.0240581593, '80': 0.0466667639, '81': 0.0466667639, '82':
        0.0466667639, '84': 0.0257905875, '85': 0.0005983956, '86':
        0.0039648716, '87': -0.0021340983, '88': -0.0021340983, '90':
        0.0157102574, '91': 0.0157102574, '92': 0.0157102574, '93':
        -0.0124584718, '94': -0.0006105312, '95': -0.0006105312, '96':
        -0.0006105312, '97': 0, '98': 0, '99': 0}
