from __future__ import print_function
import os, sys, atexit, timeit
from multiprocessing import Process, Queue, current_process
import subprocess
from collections import namedtuple
import grass.script as gscript

from gravity_io_model_params import params
DEBUG = params['debug']

os.environ['GRASS_VERBOSE'] = params['verbosity']

def cleanup():
    """ Delete temporary maps """

    if params['remove_tmps']:
        if gscript.find_file(temp_spatialunits_raster, element='cell')['name']:
            gscript.run_command('g.remove',
                               type_='raster',
                               name=temp_spatialunits_raster,
                               flags='f',
                               quiet=True)



def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def get_gammas_nace(params):
    # read gamma file and find min and max values
    nace_n = {}
    n_values = []
    min_n = 99999
    max_n = -1
    f = open(params['gamma_file'], 'r')
    headerline = params['headerline']
    for line in f:
        if headerline:
            headerline = False
            continue
        nace = line.split(',')[0]
        n = float(line.split(',')[1])
        nace_n[nace] = n
        n_values.append(n)
        if n < min_n:
            min_n = n
        if n > max_n:
            max_n = n

    f.close()
    n_values.sort()

    min_n = percentile(n_values, params['min_perc'])
    max_n = percentile(n_values, params['max_perc'])
    range_n=max_n-min_n
    range_gamma=params['max_gamma']-params['min_gamma']

    # calculate gamma value per NACE
    gammas_nace = {}
    for nace, n in nace_n.iteritems():
        if params['gammadir'] == -1:
            if n <= min_n:
                gammas_nace[nace] = params['max_gamma']
            elif n > max_n:
                gammas_nace[nace] = params['min_gamma']
            else:
                gammas_nace[nace] = params['max_gamma'] - (float(n - min_n) / range_n * range_gamma)
        else:
            if n < min_n:
                gammas_nace[nace] = params['min_gamma']
            elif n > max_n:
                gammas_nace[nace] = params['max_gamma']
            else:
                gammas_nace[nace] = params['min_gamma'] + (float(n - min_n) / range_n * range_gamma)

    return gammas_nace

def get_nace_export_investment_shares(params):
    '''Gets data from file and creates dictionary of dictionaries of
        export and investment shares of naces'''

    export_investment_shares = {}
    firstline = True
    with open(params['nace_export_investment_shares_file'], 'r') as fin:
        for line in fin:
            if firstline:
                firstline = False
                continue
            data = line.rstrip().split('|')
            d = {}
            nace = data[params['nace_col']]
            d['investment'] = float(data[params['investment_col']])
            d['export'] = float(data[params['export_col']])
            export_investment_shares[nace] = d

    return export_investment_shares

def worker(params, nace_queue, output_queue):
    try:
        pid = os.getpid()
        if params['processes'] > 1:
            params['trade_matrix_employment_file'] = params['trade_matrix_employment_file'] % pid
        for nace in iter(nace_queue.get, 'STOP'):

            bigtic = timeit.default_timer()
            #gscript.info("Working on NACE %s" % nace)
            print("Working on NACE %s" % nace)

            try:
                gamma = params['gammas_nace'][nace]
                gscript.verbose("Gamma of NACE %s = %f" % (nace, gamma))
            except:
                gscript.message("No gamma value found for NACE %s" % nace)
                output_queue.put([nace, None])
                continue

            # If we use the version where firms are grouped by pixel,
            # we have to identify each map by NACE code
            if params['firms_grouped_by_pixel']:
                point_map = params['pointmap'] + nace
            else:
                point_map = params['pointmap']

            # Get total volume of this NACE 5 and scale the NACE 2 consumption map to that total volume.
            # Do not take into account the share of the production that is exported outside the country
            # or invested into capital accumulation
            if params['firms_grouped_by_pixel']:
                total_volume = float(gscript.read_command('v.db.select',
                                                         flags='c',
                                                         map_=point_map,
                                                         column="sum(%s)" % params['volume_measure'],
                                                         quiet=True).rstrip())
            else:
                total_volume = float(gscript.read_command('v.db.select',
                                                         flags='c',
                                                         map_=point_map,
                                                         column="sum(%s)" % params['volume_measure'],
                                                         where="%s='%s'" % (params['nace_info_column'], nace),
                                                         quiet=True).rstrip())

            total_export = total_volume * params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['export']
            total_investment = total_volume * params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['investment']
            total_volume = total_volume - total_export - total_investment

            # Should we use a specific consumption 'population' map per NACE
            # or one general map for all (e.g. simple population)
            if params['use_per_nace_pop']:
                dest_map_unscaled = params['destination_map'] + nace[:params['dest_map_nace_precision']]
            else:
                dest_map_unscaled = params['destination_map']

    	    dest_map = "gravity_io_tmp_scaled_pop_%d" % pid


            map_stats = gscript.parse_command('r.univar',
                                              flags = "g",
                                              map = dest_map_unscaled)

            mapcalc_expression = "%s = " % dest_map
    	    mapcalc_expression += "%s * " % dest_map_unscaled
            mapcalc_expression += "float(%s) / %s" % (total_volume, map_stats['sum'])
            gscript.run_command('r.mapcalc',
                                expression=mapcalc_expression,
                                overwrite=True,
                                quiet=True)

            if DEBUG:
                unscaled_sum = map_stats['sum']
                map_stats = gscript.parse_command('r.univar',
                                                  flags = "g",
                                                  map = dest_map)
                print("total production employment: %d, sum of unscaled NACE 2 consumption map: %s, sum of scaled consumption map: %.3f" % (total_volume, unscaled_sum, float(map_stats['sum'])))


            # Now get the data firm by firm (or pixel by pixel)
            query = "SELECT cat, x, y, %s, %s" % (params['volume_measure'], params['spatial_unit_name'])
            query += " FROM %s" % point_map
            query += " WHERE %s>0" % params['volume_measure']
            if not params['firms_grouped_by_pixel']:
                query += " AND %s='%s'" % (params['nace_info_column'], nace)

            database = gscript.vector_db(point_map)[1]['database']
            firms_data = gscript.read_command('db.select',
                                        flags = 'c',
                                        sql = query,
                                        database = database)
            
            if len(firms_data) == 0:
                continue

            # We assume that in sectors with more than 5 firms,
            # only firms that have a volume above a certain
            # percentile of volumes in the sector actually export.
            # Calculate volume threshold value, sum total volume of above
            # threshold firms, and estimate export share for those firms as
            # total_export / total volume of firms above threshold volume
            # as long as total_export + investment of those firms is not higher
            # than their total volume (i.e. we assume that investment share is equal
            # across all sizes of firms).
            # If necessary we reduce the threshold value by a step of 0.1 until we have
            # sufficient volume.

            if len(firms_data.splitlines()) > 5:
                volumes=[]
                for firm in firms_data.splitlines():
                    volume = float(firm.split('|')[3])
                    volumes.append(volume)

                if max(volumes) == min(volumes):
                    volume_threshold = 0
                    export_share = params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['export']
                else:
                    volumes.sort()
                    volume_threshold = percentile(volumes, params['volume_perc'])
                    export_firms_total_volume = sum([v for v in volumes if v > volume_threshold])

                    thresh_percentile = params['volume_perc']
                    inv_share = params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['investment']

                    while export_firms_total_volume < (export_firms_total_volume * inv_share + total_export):
                        thresh_percentile -= 0.1
                        volume_threshold = percentile(volumes, thresh_percentile)
                        export_firms_total_volume = sum([v for v in volumes if v > volume_threshold])

                    export_share = total_export / export_firms_total_volume
            else:
                volume_threshold = 0
                export_share = params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['export']

            if DEBUG:
                print("volume threshold: %f" % volume_threshold)
                print("export share: %f" % export_share)

            firm_cats = []
            firm_spatial_units = []
            firm_volumes = []
            firm_exports = []
            firm_investments = []

            gscript.verbose("Calculating distance maps for NACE %s" % nace)
            tempdist = 'gravity_io_tmp_dist_%s_%d' % (nace, pid)

            # Starting the first loop to get data firm by firm (or pixel by
            # pixel) and to calculate distance maps and their derivatives
            # which stay constant over the entire estimation
            tic = timeit.default_timer()
            for firm in firms_data.splitlines():
                cat = int(firm.split('|')[0])
                firm_cats.append(cat)
                x = firm.split('|')[1]
                y = firm.split('|')[2]
                spatial_unit = firm.split('|')[4]
                firm_spatial_units.append(spatial_unit)
                volume = float(firm.split('|')[3])
                if volume > volume_threshold:
                    export = volume * export_share
                    firm_exports.append(export)
                else:
                    export = 0
                    firm_exports.append(export)
                investment = volume * params['export_investment_shares'][nace[:params['exp_inv_nace_precision']]]['investment']
                firm_investments.append(investment)
                volume = volume - export - investment
                firm_volumes.append(volume)
            
                if gamma > 0:
                    # Calculate distance weighted firm rate for each pixel
                    # If distance is 0, use fixed internal distance (parameter)

                    mapcalc_expression = "eval( "
                    mapcalc_expression += "tmpdist = sqrt((x()-%s)^2 + (y()-%s)^2))\n" % (x, y)
                    mapcalc_expression += "%s = if(tmpdist < %f, %f, tmpdist)" % (tempdist, params['internal_distance'], params['internal_distance'])
                    gscript.run_command('r.mapcalc',
                                        expression=mapcalc_expression,
                                        overwrite=True,
                                        quiet=True)

                    # Now create a map with the inverse distance exposed to gamma

                    tempdistexp = 'gravity_io_tmp_dist_exp_%s_%d_%d' % (nace, cat, pid)
                    mapcalc_expression = "%s = " % tempdistexp
                    mapcalc_expression += "1.0 / (%s ^ %f)" % (tempdist, gamma)
                    gscript.run_command('r.mapcalc',
                                        expression=mapcalc_expression,
                                        overwrite=True,
                                        quiet=True)


            del firms_data

            toc = timeit.default_timer()
            gscript.verbose("NACE %s: Firms: %d, Data preparation time: %f" % (nace, len(firm_cats), toc-tic))

            if gamma > 0:
                # Now we start the second loop over firms/pixels which will allow
                # estimating the A and B coefficients in the doubly-constrained
                # gravity model. We iterate to approach the values of these
                # coefficients and stop when we reach either a minimum change
                # threshold (checked against the mean of the changes of the B
                # coefficient which is defined pixel by pixel) or a maximum number of iterations

                firm_rate_map = "firm_rate_%d" % (pid)
            
                # A and B are the constraining factors in the
                # doubly-constrained gravity model.
                # As each depends on the other, We set each coefficient to 1 at the start
                # and then iterate over the respective adaptations until either the difference
                # falls below a defined threshold, or we reach the maximum number of iterations.

                A = [9999]*len(firm_cats)
                Anew = [1]*len(firm_cats)
                diffA = [None]*len(firm_cats)
                indA = 'gravity_io_tmp_indA_%d' % pid
                B = 'gravity_io_tmp_B_%d' % pid
                newB = 'gravity_io_tmp_newB_%d' % pid
                mapcalc_expression = "%s = 1" % B
                gscript.run_command('r.mapcalc',
                                   expression=mapcalc_expression,
                                   overwrite=True,
                                   quiet=True)
                diffmap = 'gravity_io_tmp_diffmap_%d' %pid
                total_A_diff = 9999
                total_B_diff = 9999
                sum_rates = 'sum_rates_%d' % pid
                temp_sum_rates = 'gravity_io_tmp_sum_rates_%d' % pid

                iterations = 0
                tic = timeit.default_timer()

                gscript.verbose("Launching constraint calculation for NACE %s" % nace)

                while (total_A_diff > params['constraint_calculation_threshold'] or total_B_diff > params['constraint_calculation_threshold']) and iterations < params['constraint_calculation_max_iter']:

                    iterations += 1
                    ticiter = timeit.default_timer()

                    mapcalc_expression = "%s = 0" % sum_rates
                    gscript.run_command('r.mapcalc',
                                        expression=mapcalc_expression,
                                        overwrite=True,
                                        quiet=True)

                    for i in range(len(firm_cats)):

                        cat = firm_cats[i]

                        tempdistexp = 'gravity_io_tmp_dist_exp_%s_%s_%d' % (nace, cat, pid)

                        mapcalc_expression = "%s = %s * %s * %s" % (indA, B, dest_map, tempdistexp) 
                        gscript.run_command('r.mapcalc',
                                   expression=mapcalc_expression,
                                   overwrite=True,
                                   quiet=True)

                        map_stats = gscript.parse_command('r.univar',
                                                          flags = 'g',
                                                          map_ = indA,
                                                          quiet=True)

                        Anew[i] = (1.0 / float(map_stats['sum']))
                        diffA[i] = float(abs(A[i]-Anew[i]))/A[i]


                        mapcalc_expression = "%s = %s + %.10f * %.10f * %s\n" % (temp_sum_rates, sum_rates, Anew[i], firm_volumes[i], tempdistexp)
                        gscript.run_command('r.mapcalc',
                                            expression=mapcalc_expression,
                                            overwrite=True,
                                            quiet=True)


                        gscript.run_command('g.rename',
                                            raster=[temp_sum_rates,sum_rates],
                                            overwrite=True,
                                            quiet=True)


                    A = list(Anew)


                    mapcalc_expression = "%s = 1.0 / %s" % (newB, sum_rates)
                    gscript.run_command('r.mapcalc',
                               expression=mapcalc_expression,
                               overwrite=True,
                               quiet=True)


                    mapcalc_expression = "%s = float(abs(%s - %s))/%s" % (diffmap, B, newB, B)
                    gscript.run_command('r.mapcalc',
                               expression=mapcalc_expression,
                               overwrite=True,
                               quiet=True)

                    map_stats = gscript.parse_command('r.univar',
                                                      flags = 'g',
                                                      map_ = diffmap,
                                                      quiet=True)

                    total_B_diff = float(map_stats['sum'])
                    mean_B_diff = float(map_stats['mean'])
                    total_A_diff = sum(diffA)
                    mean_A_diff = total_A_diff/len(diffA)

                    if DEBUG:
                        map_stats = gscript.parse_command('r.univar',
                                                          flags = 'g',
                                                          map_ = newB,
                                                          quiet=True)
                        meanB = float(map_stats['mean'])
                        meanA = float(sum(A))/len(A)
                        print("\nIteration: %d" % iterations)
                        print("mean A: %g, mean B: %g" % (meanA, meanB))
                        print("total A diff : %g, mean A diff: %g, total B diff : %g, mean B diff: %g" % (total_A_diff, mean_A_diff, total_B_diff, mean_B_diff))

                    gscript.run_command('g.rename',
                                        raster=[newB,B],
                                        overwrite=True,
                                        quiet=True)

                    tociter = timeit.default_timer()
                    if DEBUG:
                        print("Time for iteration %d : %f seconds" % (iterations, tociter-ticiter))

                if params['remove_tmps']:

                    gscript.run_command('g.remove',
                                       type_ = 'raster',
                                       name = sum_rates,
                                       flags = 'f',
                                       quiet = True)

                toc = timeit.default_timer()
                gscript.verbose("Finished constraint calculations for NACE %s in %f seconds" % (nace, toc-tic))
                if iterations == params['constraint_calculation_max_iter']:
                    gscript.warning("Too many iterations for NACE %s ! total_A_diff = %g, total_B_diff = %g" % (nace, total_A_diff, total_B_diff))

            gscript.verbose("Calculating trade matrix for NACE %s" % nace)
            tic = timeit.default_timer()

            # Now that we have values for A and B we apply them in the
            # doubly-constrained gravity formula to estimate the trade flows

            spatial_units_trade_matrix_employment = {}
            firm_matrix_map = 'firm_matrix_%d' % pid
            for i in range(len(firm_cats)):
               
                cat = firm_cats[i]
 
                if gamma > 0:
                    # When gamma is > 0 apply constrained gravity formula
                    tempdistexp = 'gravity_io_tmp_dist_exp_%s_%s_%d' % (nace, cat, pid)
                    mapcalc_expression = "%s = %e * %s * %s * %s * %s" % (firm_matrix_map, A[i], B, firm_volumes[i], dest_map, tempdistexp) 

                else:
                    # When gamma = 0, distance plays no role and we just distribute 
                    # the production of the firm to all pixels according to their 
                    # share in consumtion population
                    map_stats = gscript.parse_command('r.univar',
                                                      flags = "g",
                                                      map = dest_map)
                    mapcalc_expression = "%s = %s * (float(%s)/%s)" % (firm_matrix_map, firm_volumes[i], dest_map, map_stats['sum'])

                gscript.run_command('r.mapcalc',
                                   expression = mapcalc_expression,
                                   overwrite = True,
                                   quiet=True)

                map_stats = gscript.parse_command('r.univar',
                                                  flags = "g",
                                                  map = firm_matrix_map)

                spatial_unit = firm_spatial_units[i]
                sum_pop = float(map_stats['sum'])

                # Aggregate the export employment by pixel to given spatial units
                map_stats = gscript.read_command('r.univar',
                                                 flags="t",
                                                 map=firm_matrix_map,
                                                 zones=params['spatial_units_rast'],
                                                 separator='pipe',
                                                 quiet=True)

                firm_trade_matrix = {}
                first = True
                for line in map_stats.splitlines():
                    if first:
                        first = False
                        continue
                    data = line.split('|')
                    firm_trade_matrix[data[0]] = float(data[12])

                # We add the data of the firm to the spatial unit the firm is
                # located in
                if spatial_unit in spatial_units_trade_matrix_employment:
                    spatial_units_trade_matrix_employment[spatial_unit]['export'] += firm_exports[i]
                    spatial_units_trade_matrix_employment[spatial_unit]['investment'] += firm_investments[i]
                    for target_unit in firm_trade_matrix:
                        if target_unit in spatial_units_trade_matrix_employment[spatial_unit]:
                            spatial_units_trade_matrix_employment[spatial_unit][target_unit] += ( firm_trade_matrix[target_unit] / sum_pop ) * float(firm_volumes[i])
                        else:
                            spatial_units_trade_matrix_employment[spatial_unit][target_unit] = ( firm_trade_matrix[target_unit] / sum_pop ) * float(firm_volumes[i])
                else:
                    spatial_units_trade_matrix_employment[spatial_unit] = {}
                    spatial_units_trade_matrix_employment[spatial_unit]['export'] = firm_exports[i]
                    spatial_units_trade_matrix_employment[spatial_unit]['investment'] = firm_investments[i]
                    for target_unit in firm_trade_matrix:
                        spatial_units_trade_matrix_employment[spatial_unit][target_unit] = ( firm_trade_matrix[target_unit] / sum_pop ) * float(firm_volumes[i])

            toc = timeit.default_timer()
            gscript.verbose("Finished calculating trade matrix for NACE %s in %f seconds" % (nace, toc-tic))
                            
            if DEBUG:
                gisdbase = gscript.gisenv()['GISDBASE']
                du = subprocess.Popen(["du", "-sh", gisdbase], stdout=subprocess.PIPE)
                du_output=du.communicate()[0].rstrip()
                gscript.warning("NACE: %s, Disk usage: %s" % (nace, du_output))

            # Now remove the large number of temporary maps created during the
            # process
            if params['remove_tmps']:

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = firm_matrix_map,
                                   flags = 'f',
                                   quiet = True)

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = dest_map,
                                   flags = 'f',
                                   quiet = True)

                if gamma > 0:
                    gscript.run_command('g.remove',
                                       type_ = 'raster',
                                       pattern = 'gravity_io_tmp_*_%d' % pid,
                                       flags = 'f',
                                       quiet = True)


            gscript.verbose('Writing results to files')

            # In order to avoid race conditions when writing to the output file,
            # each parallel process gets its own file to write to.
            # These files need to be merged after the model run.
            with open(params['trade_matrix_employment_file'], 'a') as f:
                for orig_unit in spatial_units_trade_matrix_employment:
                    for dest_unit in spatial_units_trade_matrix_employment[orig_unit]:
                        output_string = nace + ';'
                        output_string += orig_unit + ';'
                        output_string += dest_unit + ';'
                        output_string += str(spatial_units_trade_matrix_employment[orig_unit][dest_unit]) + '\n'
                        f.write(output_string)

            bigtoc = timeit.default_timer()
            gscript.info("Finished with NACE %s in %f seconds" % (nace, bigtoc-bigtic))

            output_queue.put([nace, 'OK'])


    except:
        gscript.warning("Error in worker script:")
        raise
        

    return True


def main():

    if params['array']:
        run = sys.argv[1]
        params['nace_codes_file'] = params['nace_codes_file'] % run
        params['trade_matrix_employment_file'] = params['trade_matrix_employment_file'] % run


    # Create the dictionaries with gamma coefficients and shares of
    # international export and investment by NACE
    params['gammas_nace'] = get_gammas_nace(params)
    if params['nace_gamma_file']:
        with open(params['nace_gamma_file'], 'w') as fout:
            for nace in params['gammas_nace']:
                fout.write("%s;%s\n" % (nace, params['gammas_nace'][nace]))

    params['export_investment_shares'] = get_nace_export_investment_shares(params)

    # define region and mask
    gscript.run_command('g.region',
                        vect=params['mask_vector'],
                        res=params['resolution'],
                        flags='a',
                        quiet=True)
    gscript.run_command('r.mask',
                        vect=params['mask_vector'],
                        overwrite=True,
                        quiet=True)

    # remove an existing results file of the same name, as we will append to file, not overwrite it
    if params['remove_old_results_file']:
        gscript.try_remove(params['trade_matrix_employment_file'])

    # Create a raster with the spatial units to be able to use r.univar zones=
    global temp_spatialunits_raster
    pid = os.getpid()
    temp_spatialunits_raster = 'temp_spatial_units_rast_%d' % pid
    gscript.run_command('v.to.rast',
                        input_=params['spatial_unit_map'],
                        output=temp_spatialunits_raster,
                        use='attr',
                        attribute_column=params['spatial_unit_code'],
                        overwrite=True,
                        quiet=True)
    params['spatial_units_rast'] = temp_spatialunits_raster

    # Launch the parallel processing NACE by NACE
    processes_list = []
    nace_queue = Queue()
    output_queue = Queue()

    with open(params['nace_codes_file'], 'r') as nace_file:
        data = [line.strip() for line in nace_file]

    for nace in data:
        nace_queue.put(nace) 

    for p in xrange(params['processes']):
        proc = Process(target=worker, args=(params, nace_queue, output_queue))
        proc.start()
        processes_list.append(proc)
        nace_queue.put('STOP')
    for p in processes_list:
        p.join()
    output_queue.put('STOP')

    # Finally remove the mask
    gscript.run_command('r.mask', flags='r', quiet=True)

if __name__ == "__main__":
        atexit.register(cleanup)
        main()

