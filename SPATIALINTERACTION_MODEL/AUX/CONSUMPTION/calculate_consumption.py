import os
import grass.script as gscript

def create_consumption_dict(filename, sep = '|'):
    '''Read csv file with consumption shares and transform
	into a dictionary'''

    consumption_dict = {}

    firstline = True
    with open(filename, 'r') as fin:
	for line in fin:
	    data = line.rstrip().split(sep)
	    if firstline:
		headers = data[1:]
		firstline = False
		continue

	    nace = data[0]
	    i = 1
	    d = {}
	    for header in headers:
		d[header] = data[i]
		i += 1
	    consumption_dict[nace] = d
    return consumption_dict

for annee in [2010, 2011, 2012, 2013, 2014]:

    print "Treating %d\n" % annee
    # These maps and columns have to exist in the current search path of the
    # GRASS GIS mapset
    firms_map = 'produnits_%d' % annee
    nace_column = 'cd_nace_%d' % annee
    volume_column = 'turnover_estim'
    population_rel_map = 'population_relative_%d' % annee

    # Create a dictionary of consumption by sector
    # The input file has to follow the following format:
    # nace_prod|nace_cons1|nace_cons2|nace_cons3|...|Local_intermediate|Local_final|Investment|Export
    # where nace_prod = code sector of production, nace_cons1 = code sector of
    # consumption and the nace_cons* columns contain the share of each sector in
    # total local (national) intermediate consumption, Local_intermediate and
    # Local_final contain the respective share of intermediate and final
    # consumption in total local consumption, Investment=Share of investment
    # in total production, Export=share of export in total production
    # This means that Total production - Investment - Export = Total local
    # consumption

    consumption_dict = create_consumption_dict('../io_shares.csv')

    tempmap = 'temp_tempmap_%d' % os.getpid()

    for nace2 in gscript.read_command('v.db.select',
                                       map=firms_map,
                                       column="substr(%s, 1, 2)" % nace_column,
                                       group="substr(%s, 1, 2)" % nace_column,
                                       where="%s <> '' AND %s > 0" % (nace_column, volume_column),
                                       flags='c',
                                       quiet=True).splitlines():
            print nace2
            sql = "SELECT sum(%s) FROM %s" % (volume_column, firms_map)
            sql += " where substr(%s, 1, 2) = '%s'" % (nace_column, nace2)
            db = gscript.vector_db(firms_map)[1]['database']
            total_volume = float(gscript.read_command('db.select',
                                                    sql=sql,
                                                    database=db,
                                                    flags='c',
                                                    quiet=True).rstrip())

            export_volume = float(consumption_dict[nace2].pop('Export')) * total_volume
            investment_volume = float(consumption_dict[nace2].pop('Investment')) * total_volume
            local_volume = total_volume - export_volume - investment_volume
            gscript.message("nace: %s, total: %f, local: %f" % (nace2, total_volume, local_volume))
            final_cons_volume = float(consumption_dict[nace2].pop('Local_final')) * local_volume

            interm_cons_volume = float(consumption_dict[nace2].pop('Local_intermediate')) * local_volume
            intermediate_consumption_map = 'temp_intermediate_consumption_%s' % os.getpid()
            gscript.run_command('r.mapcalc',
                                expression="%s = 0" % intermediate_consumption_map,
                                quiet=True,
                                overwrite=True)

            for nace in consumption_dict[nace2]:
                turnover_rel_map = "turnover_rel_%d_%s" % (annee, nace)
                mapcalc_expression = "%s = " % tempmap
                mapcalc_expression += "%s + " % intermediate_consumption_map
                mapcalc_expression += "(%s *" % turnover_rel_map
                mapcalc_expression += "%f * %f)" % (float(consumption_dict[nace2][nace]), interm_cons_volume)

                gscript.run_command('r.mapcalc',
                                    expression=mapcalc_expression,
                                    quiet=True,
                                    overwrite=True)

                gscript.run_command('g.rename',
                                   raster=[tempmap, intermediate_consumption_map],
                                   quiet=True,
                                   overwrite=True)

            total_consumption_map = 'consumption_%d_%s' % (annee, nace2)
            mapcalc_expression = "%s = " % total_consumption_map
            mapcalc_expression += "%s * %f + " % (population_rel_map, final_cons_volume)
            mapcalc_expression += "%s" % intermediate_consumption_map
            gscript.run_command('r.mapcalc',
                                expression=mapcalc_expression,
                                quiet=True,
                                overwrite=True)

            gscript.run_command('g.remove',
                                type_='raster',
                                name=[tempmap, intermediate_consumption_map],
                                flags='f',
                                quiet=True)

