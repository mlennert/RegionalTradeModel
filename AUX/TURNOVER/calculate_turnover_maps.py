import os
import grass.script as gscript


for annee in [2010, 2011, 2012, 2013, 2014]:
    print "Treating %d\n" % annee
    # These map and columns have to exist in the current mapset's search path
    firms_map = 'produnits_%d' %annee
    nace_column = 'cd_nace_%d' % annee
    turnover_column = 'turnover_estim'
    for nace2 in gscript.read_command('v.db.select',
                                       map=firms_map,
                                       column="substr(%s, 1, 2)" % nace_column,
                                       group="substr(%s, 1, 2)" % nace_column,
                                       where="%s <> ''" % nace_column,
                                       flags='c',
                                       quiet=True).splitlines():
            print nace2
            pin = gscript.pipe_command('v.db.select',
                                       map = firms_map,
                                       column="x,y,%s" % turnover_column, 
                                       where="substr(%s, 1, 2) = '%s' AND %s >= 0" % (nace_column, nace2, turnover_column),
                                       flags='c',
                                       quiet=True)
            total_turnover_map = 'turnover_%d_%s' % (annee, nace2)
            p = gscript.start_command('r.in.xyz',
                                      input_='-',
                                      stdin=pin.stdout,
                                      method='sum',
                                      type_='DCELL',
                                      output=total_turnover_map,
                                      quiet=True,
                                      overwrite=True)
            if p.wait() is not 0:
                gscript.fatal("Error in r.in.xyz with nace %s" % nace2)

            stats = gscript.parse_command('r.univar',
                                          map_=total_turnover_map,
                                          flags='g',
                                          quiet=True)
            
            relative_turnover_map = 'turnover_rel_%d_%s' % (annee, nace2)
            
            mapcalc_expression = "%s = " % relative_turnover_map
            
            mapcalc_expression += "%s / " % total_turnover_map
            
            mapcalc_expression += "%f" % float(stats['sum'])
            
            gscript.run_command('r.mapcalc',
                                expression=mapcalc_expression,
                                quiet=True,
                                overwrite=True)

    # This dictionary represents combinations of sectors used in the IO-tables
    # and for which turnover are also necessary
    combidict = {'05_09' : ['05', '07', '08', '09'], 
                 '10_12' : ['10', '11', '12'],
                 '13_15' : ['13', '14', '15'],
                 '31_32' : ['31', '32'],
                 '37_39' : ['37', '38', '39'],
                 '41_43' : ['41', '42', '43'],
                 '55_56' : ['55', '56'],
                 '59_60' : ['59', '60'],
                 '62_63' : ['62', '63'],
                 '69_70' : ['69', '70'],
                 '74_75' : ['74', '75'],
                 '80_82' : ['80', '81', '82'],
                 '87_88' : ['87', '88'],
                 '90_92' : ['90', '91', '92']}

    for combi in combidict:
        print combi
        total_turnover_map = 'turnover_%d_%s' % (annee, combi)
        turnovermaps = []
        for nace in combidict[combi]:
            name = 'turnover_%d_%s' % (annee, nace)
            turnovermaps.append(name)
        gscript.run_command('r.series',
                            input_=turnovermaps,
                            output=total_turnover_map,
                            method='sum',
                            overwrite=True,
                            quiet=True)

                            
        stats = gscript.parse_command('r.univar',
                                      map_=total_turnover_map,
                                      flags='g',
                                      quiet=True)
        
        relative_turnover_map = 'turnover_rel_%d_%s' % (annee, combi)
        
        mapcalc_expression = "%s = " % relative_turnover_map
        
        mapcalc_expression += "%s / " % total_turnover_map
        
        mapcalc_expression += "%f" % float(stats['sum'])
        
        gscript.run_command('r.mapcalc',
                            expression=mapcalc_expression,
                            quiet=True,
                            overwrite=True)
