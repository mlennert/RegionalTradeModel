from __future__ import print_function
import os, sys, atexit
from multiprocessing import Process, Queue, current_process
import grass.script as gscript

from calculate_huff_params import params

os.environ['GRASS_VERBOSE'] = params['verbosity']

'''def cleanup():
    """ Delete temporary maps """

    if params['remove_tmps']:
	gscript.run_command('g.remove',
			   type_ = 'raster',
			   pattern = 'firm_rate_%d_*' % pid,
			   flags = 'f',
			   quiet = True)

	gscript.run_command('g.remove',
			   type_ = 'raster',
			   pattern = 'firm_pop_%d_*' % pid,
			   flags = 'f',
			   quiet = True)

	gscript.run_command('g.remove',
			   type_ = 'raster',
			   name = tempprob,
			   flags = 'f',
			   quiet = True)

	gscript.run_command('g.remove',
			   type_ = 'raster',
			   name = sum_rates,
			   flags = 'f',
			   quiet = True)

	gscript.run_command('g.remove',
			   type_ = 'raster',
			   name = tempdist,
			   flags = 'f',
			   quiet = True)

	gscript.run_command('g.remove',
			   type_ = 'raster',
			   name = tempdist0,
			   flags = 'f',
			   quiet = True)'''



def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def worker(params, nace_queue, output_queue):
    pid = os.getpid()
    try:
        for nace in iter(nace_queue.get, 'STOP'):

            gscript.info("Working on NACE %s" % nace)

            tempdist0 = 'tempdist0_%d' % pid
            tempdist = 'tempdist_%d' % pid

            try:
                gamma = params['gammas_nace'][nace]
                gscript.verbose("Gamma of NACE %s = %f" % (nace, gamma))
            except:
                gscript.message("No gamma value found for NACE %s" % nace)
                output_queue.put([nace, None])
                continue

            gscript.verbose("Calculating individual rate maps for NACE %s" % nace)

            query = "SELECT cat, x, y, %s, %s" % (params['volume_measure'], params['spatial_unit_name'])
            query += " FROM %s" % params['pointmap']
            query += " WHERE %s>0" % params['volume_measure']
            query += " AND %s='%s'" % (params['nace_info_column'], nace)

            database = gscript.vector_db(params['pointmap'])[1]['database']
            firms = gscript.read_command('db.select',
                                        flags = 'c',
                                        sql = query,
                                        database = database)
            
            if len(firms) == 0:
                continue

            firm_info = {}
            rate_maps = []
            total_pop_spatial_unit = {}
            total_empl_spatial_unit = {}
            total_empl_outside_spatial_unit = {}
            firm_totals_filename=gscript.tempfile()
            x = {}
            y = {}
            for firm in firms.splitlines():
                cat = firm.split('|')[0]
                x[cat] = firm.split('|')[1]
                y[cat] = firm.split('|')[2]
                volume = firm.split('|')[3]
                spatial_unit = firm.split('|')[4]
                firm_info[cat] = spatial_unit

                mapcalc_expression = "%s = sqrt( (x()-%s)^2 + (y()-%s)^2 )" % (
                                    tempdist0, x[cat], y[cat])

                gscript.run_command('r.mapcalc',
                                   expression=mapcalc_expression,
                                   overwrite=True,
                                   quiet=True)

                mapcalc_expression = "%s = if(%s == 0, %f, %s)" % (tempdist,
                        tempdist0, float(params['resolution'])/2, tempdist0)

                gscript.run_command('r.mapcalc',
                                   expression=mapcalc_expression,
                                   overwrite=True,
                                   quiet=True)

                firm_rate_map = "firm_rate_%d_%s" % (pid, cat)
                rate_maps.append(firm_rate_map)
                mapcalc_expression = "%s = float(%s) / exp(%s, %F)" % (firm_rate_map, volume, tempdist, gamma)

                gscript.run_command('r.mapcalc',
                                   expression=mapcalc_expression,
                                   overwrite=True,
                                   quiet=True)

            gscript.debug("Calculating sum of rates for NACE %s" % nace)

            sum_rates = 'sum_rates_%d' % pid

            fname=gscript.tempfile()
            f_rate_maps=open(fname, 'w')
            for rate_map in rate_maps:
                print(rate_map, file=f_rate_maps)
            f_rate_maps.close()


            gscript.run_command('r.series',
                               file_=fname,
                               output=sum_rates,
                               method='sum',
                               overwrite=True,
                               quiet=True)

            gscript.try_remove(fname)

            gscript.verbose("Calculating probabilities and population concerned for NACE %s" % nace)

            tempprob = 'temp_prob_%d' % pid
            for firm_rate_map in rate_maps:

                mapcalc_expression = "%s = float(%s) / float(%s)" % (tempprob, firm_rate_map, sum_rates)

                gscript.run_command('r.mapcalc',
                                   expression = mapcalc_expression,
                                   overwrite = True,
                                   quiet=True)

                if params['remove_tmps']:
                    gscript.run_command('g.remove',
                                       type_ = 'raster',
                                       name = firm_rate_map,
                                       flags = 'f',
                                       quiet = True)

                cat = firm_rate_map.split('_')[-1] 
                firm_pop_map = 'firm_pop_%d' % pid

                nace2 = nace[:2]
                if params['inc_tb']:
                    trade_balance = params['trade_balance_over_output_by_nace'][nace2]
                    mapcalc_expression = "%s = %s * (%s * (1 - %f))" % (firm_pop_map, params['consumption_population_map'], tempprob, trade_balance)
                else:
                    mapcalc_expression = "%s = %s * %s" % (firm_pop_map, tempprob, params['population_map']) 

                gscript.run_command('r.mapcalc',
                                   expression = mapcalc_expression,
                                   overwrite = True,
                                   quiet=True)

                pop_map_stats = gscript.parse_command('r.univar',
                                                     flags = "g",
                                                     map = firm_pop_map)

                spatial_unit = firm_info[cat]
                sum_pop = float(pop_map_stats['sum'])
                if spatial_unit in total_pop_spatial_unit:
                    total_pop_spatial_unit[spatial_unit] += sum_pop
                else:
                    total_pop_spatial_unit[spatial_unit] = sum_pop

                if params['calc_consumption_pop_indicator']:
                    # Calculate the total employment for exports
                    # total employment x share of consumption population that is
                    # outside the spatial unit the firm is in
                    tempraster = 'huff_tempraster_%s_%s' % (spatial_unit, pid)
                    gscript.run_command('v.to.rast',
                                        input_=params['spatial_unit_map'],
                                        where="%s <> '%s' " % (params['spatial_unit_name_map'], spatial_unit),
                                        output=tempraster,
                                        use='val',
                                        overwrite=True,
                                        quiet=True)
                    pop_map_stats = gscript.parse_command('r.univar',
                                                         flags="g",
                                                         map=firm_pop_map,
                                                         zones=tempraster)

                    sum_pop_outside = float(pop_map_stats['sum'])
                    prop_outside = sum_pop_outside / sum_pop
                    total_empl = int(gscript.read_command('v.db.select',
                                                      map_=params['pointmap'],
                                                      column=params['volume_measure'],
                                                      where='cat = %d' % int(cat),
                                                      flags='c',
                                                      quiet=True))
                    if spatial_unit in total_empl_spatial_unit:
                        total_empl_spatial_unit[spatial_unit] += total_empl
                    else:
                        total_empl_spatial_unit[spatial_unit] = total_empl

                    exp_empl = int(total_empl) * prop_outside
                    if spatial_unit in total_empl_outside_spatial_unit:
                        total_empl_outside_spatial_unit[spatial_unit] += exp_empl
                    else:
                        total_empl_outside_spatial_unit[spatial_unit] = exp_empl

                    gscript.run_command('g.remove',
                                        type='rast',
                                        name=tempraster,
                                        flags='f',
                                        quiet=True)

                if(params['calculate_total_consumption_potentials']):                
                    firm_total = "%s|%s|%d\n" % (x[cat], y[cat], sum_pop)
                    with open(firm_totals_filename, 'a') as f_firm_totals:
                        f_firm_totals.write(firm_total)

            if params['remove_tmps']:
                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = tempprob,
                                   flags = 'f',
                                   quiet = True)

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = firm_pop_map,
                                   flags = 'f',
                                   quiet = True)

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = sum_rates,
                                   flags = 'f',
                                   quiet = True)

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = tempdist,
                                   flags = 'f',
                                   quiet = True)

                gscript.run_command('g.remove',
                                   type_ = 'raster',
                                   name = tempdist0,
                                   flags = 'f',
                                   quiet = True)


            gscript.verbose('Writing results to files')
	    with open(params['pop_by_spatial_unit_and_nace_file'], 'a') as f:
                for spatial_unit, pop in total_pop_spatial_unit.iteritems():
                    output_string = nace + ';' + spatial_unit + ';' + str(pop) + '\n'
                    f.write(output_string)

            if params['calc_consumption_pop_indicator']:
                with open(params['empl_by_spatial_unit_and_nace_file'], 'a') as f:
                    for spatial_unit, pop in total_empl_outside_spatial_unit.iteritems():
                        output_string = nace + ';'
                        output_string += spatial_unit + ';'
                        output_string += str(total_empl_spatial_unit[spatial_unit]) + ';'
                        output_string += str(pop) + '\n'
                        f.write(output_string)

            output_queue.put([nace, 'OK'])

            if(params['calculate_total_consumption_potentials']):
                tot_cons_potential_map = 'total_consumption_potential_%s_%s' % (params['pointmap'], nace)
                gscript.run_command('r.in.xyz',
                                   input_ = firm_totals_filename,
                                   method = 'sum',
                                   output = tot_cons_potential_map,
                                   overwrite = True,
                                   quiet = True)
                gscript.try_remove(firm_totals_filename)

            gscript.info("Finished with NACE %s" % nace)


    except:
        output_queue.put([nace, None])
        

    return True


def main():

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

    params['gammas_nace'] = gammas_nace

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
	gscript.try_remove(params['pop_by_spatial_unit_and_nace_file'])
	gscript.try_remove(params['empl_by_spatial_unit_and_nace_file'])

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

    gscript.run_command('r.mask', flags='r', quiet=True)

if __name__ == "__main__":
	'''atexit.register(cleanup)'''
        main()

