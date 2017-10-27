import grass.script as gscript
import math, sys, os

gscript.run_command('g.remove', type_='rast', pat='*', flags='f')
gscript.run_command('g.remove', type_='vect', pat='*', flags='f')
# define region and mask
gscript.run_command('g.region', vect='hull', res=1000, flags='a', quiet=True)
gscript.run_command('r.mask', vect='communes', overwrite=True, quiet=True)
gscript.run_command('g.copy', vect='new_nuts@PERMANENT,local_new_nuts',
        overwrite=True, quiet=True)
mapset = gscript.parse_command('g.gisenv', flags='n')['MAPSET']
new_nuts_map = 'local_new_nuts@' + mapset
resolution = gscript.region()['ewres']

population_map = 'population'
total_population_stats = gscript.parse_command('r.univar',
                                     flags = "g",
                                     map = population_map)
total_population = float(total_population_stats['sum'])
total_income_map= 'rev_total_by_km2_popbased'
total_income_stats = gscript.parse_command('r.univar',
                                     flags = "g",
                                     map = total_income_map)
total_income = float(total_income_stats['sum'])
use_income = True

avg_exp_value = {}

# Should distance be different for each NACE ?
spec_distance = True
# Should we use gaussian filter ?
gauss = False

# Names of temporary maps
neigh_map_nace = 'neigh_map_nace'
neigh_map_all = 'neigh_map_all'
neigh_map_inc = 'neigh_map_inc'

if spec_distance:
    radiuses = [0]
    # Where to find mean and stddev of distances per NACE
    distance_file = '/home/mlennert/THESE/DATA/DBRIS/ANALYSE_DISTANCES/mean.csv'
    distskipfirstline = True
    stddev_file = '/home/mlennert/THESE/DATA/DBRIS/ANALYSE_DISTANCES/stddev.csv'
    stdskipfirstline = True
    # Read files
    distances = {}
    f = open(distance_file, 'r')
    for line in f:
        if distskipfirstline:
            distskipfirstline = False
            continue
        nace = line.split(',')[0]
        distance = float(line.split(',')[1])
        distances[nace] = distance
    f.close()

    stddevs = {}
    f = open(stddev_file, 'r')
    for line in f:
        if stdskipfirstline:
            stdskipfirstline = False
            continue
        nace = line.split(',')[0]
        stddev = float(line.split(',')[1])
        stddevs[nace] = stddev
    f.close()
else:
    # Set distance to 45,000 which is the median of mean distances 
    # of all points in Belgium to closest production site per NACE
    radiuses = [45]
    stddevs = [33]
    # When spec_distance = False, then it makes sense to loop through a series
    # of radiuses
    # radiuses = [x for x in range(15,200,20) if x%2 != 0]

# Get sum of all employment in Belgium
total_employment_stats = gscript.parse_command('r.univar',
                                     flags = "g",
                                     map = 'empl_total')
total_empl = float(total_employment_stats['sum'])


# Determine which list of NACEs to use (for parallel processing)
argvl = len(sys.argv)
if argvl > 1:
    run = int(sys.argv[1])
else:
    run = None

# Income map and trade balance / output per NACE (used for inc_tb)
trade_balance_over_output_by_nace = {'01': 0.0348685065, '02': 0.0348685065, '03':
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

# Small sample of NACE for test runs
group_test=['88109', '58190', '94910', '45204']

# NACE divided into groups of more or less equal number of firms,
# for parallel processing
group=[['46771', '86907', '50100', '84210', '46213', '88991', '38211',
'02300', '58210', '88103', '30120', '24410', '88109', '26120', '38221', '28490',
'49500', '23440', '65200', '28130', '14110', '20170', '10620', '28240', '65300',
'24460', '46432', '77350', '23650', '24450', '16220', '90022', '27401', '27520',
'85104', '24440', '77400', '25710', '87203', '33140', '16292', '01479', '91041',
'74102', '51210', '45401', '46441', '77394', '35300', '26700', '14191', '46691',
'10320', '02400', '25910', '24420', '14200', '10810', '26400', '43991', '20530',
'78300', '30200', '24200', '68322', '10410', '28110', '85601', '68204', '45193',
'27510', '27900', '47511', '77210', '46240', '85510', '47787', '23190', '80200',
'27320', '85325', '55900', '29202', '01462', '20150', '10391', '10730', '86909',
'43992', '11070', '85591', '25940', '85323', '21100', '93291', '77293', '66300',
'46694', '01490', '30920', '46493', '58190', '87101', '46696', '28299', '20420',
'10840', '39000', '87901', '01471', '86210', '94991', '63910', '16210', '29201',
'46421', '64992', '10850', '82910', '88101', '87909', '46341', '43130', '10890',
'45209', '46733', '46130', '01430', '20590', '21201', '60200', '87202', '82200',
'46425', '33150', '16240', '47115', '96099', '91042', '35110', '10520', '84301',
'01410', '27110', '16291', '38322', '27402', '01461', '77320', '47291', '33200',
'62090', '74201', '49100', '47990', '82920', '46520', '94910', '31010', '22230',
'17210', '84220', '59113', '73120', '58110', '47792', '93299', '84112', '23630',
'45203', '46630', '66210', '46660', '43332', '46492', '13929', '46480', '10110',
'85422', '46741', '16100', '46412', '46900', '47785', '46720', '41203', '90042',
'90023', '01301', '61200', '18130', '46450', '47762', '47810', '46392', '70100',
'47650', '69101', '23700', '46751', '47210', '82300', '32500', '94999', '47640',
'94110', '82110', '31091', '45310', '53100', '46423', '45204', '47300', '52290',
'85203', '46693', '46731', '47620', '47711', '47721', '46699', '68311', '43999',
'70220', '10712', '49410', '41201'],
['28294', '07290', '59209', '88994', '85321', '85329', '87209', '85209',
'23430', '56309', '38212', '93128', '26800', '35230', '13950', '45192', '17240',
'08910', '38120', '85319', '30990', '85429', '96094', '50300', '79909', '18200',
'32110', '33130', '28960', '31092', '11030', '09100', '23640', '24330', '88993',
'66110', '28910', '88102', '33160', '17220', '86904', '85609', '14310', '65111',
'85326', '95250', '13940', '46424', '55209', '46779', '38321', '85593', '93129',
'46472', '43993', '41202', '80300', '65112', '38310', '10312', '91020', '38213',
'11010', '28140', '95240', '90031', '84242', '19200', '35120', '29310', '63120',
'93122', '47523', '46212', '10392', '77295', '21209', '14390', '93191', '43212',
'46650', '45112', '32200', '64910', '59112', '08122', '77392', '77294', '71113',
'59203', '46411', '47526', '46383', '28295', '28150', '85312', '24340', '86903',
'42911', '55204', '23321', '94995', '59130', '28410', '42919', '14199', '60100',
'46322', '25930', '86905', '18110', '02100', '61900', '85205', '85599', '95210',
'64999', '10610', '97000', '25739', '22190', '28120', '26510', '42120', '81290',
'94994', '01620', '13300', '55203', '13921', '35140', '59114', '32123', '90021',
'20300', '45191', '51100', '46391', '94992', '10120', '10720', '10510', '47820',
'32990', '27120', '28300', '13200', '46360', '47192', '13930', '52249', '84302',
'32121', '69103', '56290', '92000', '29320', '93292', '37000', '43110', '84115',
'10711', '50400', '84113', '80100', '85314', '94120', '68321', '63110', '46331',
'79901', '47788', '46190', '61100', '69203', '56302', '85520', '42220', '58140',
'72190', '22290', '46496', '93121', '43291', '46742', '25610', '14130', '45402',
'47712', '87301', '46494', '01240', '45111', '46761', '85592', '45320', '49390',
'68201', '47714', '01610', '47191', '96011', '47252', '82990', '96031', '46499',
'88911', '47750', '88999', '46510', '43120', '43331', '18120', '47782', '43310',
'56210', '25620', '55100', '84114', '45201', '71121', '69202', '43222', '43211',
'47716', '47730', '56101'],
['11020', '24310', '93124', '28950', '05100', '77340', '23420', '35210',
'99000', '30910', '24320', '85202', '91012', '01290', '85207', '59201', '95120',
'93192', '26600', '28230', '23322', '88919', '20510', '87303', '10420', '13910',
'23620', '10920', '87204', '88992', '87304', '74104', '28292', '90029', '47519',
'23310', '77310', '23410', '93126', '64929', '58120', '96095', '77391', '85206',
'23520', '85532', '96091', '23690', '24520', '46382', '74105', '87205', '20600',
'32300', '94993', '46752', '03220', '01309', '46497', '77291', '24530', '33170',
'46695', '42212', '46735', '64300', '17230', '96092', '15200', '72200', '10860',
'01250', '90032', '10393', '47783', '23130', '77292', '14120', '47522', '03110',
'46697', '63990', '35220', '64921', '24510', '42130', '42211', '14140', '23510',
'77330', '28210', '47527', '20110', '25731', '85324', '84239', '85322', '96093',
'47513', '46473', '74901', '35130', '68312', '72110', '10200', '46640', '93211',
'47793', '86104', '38329', '46350', '84309', '46498', '47529', '45194', '25210',
'28920', '87201', '93212', '64922', '93127', '23120', '46211', '20160', '20140',
'79120', '18140', '47630', '59140', '86906', '86901', '69109', '25502', '47260',
'13960', '22220', '85103', '01450', '25290', '11050', '46140', '46433', '46491',
'55202', '45202', '47715', '71201', '43995', '94200', '01110', '58130', '47111',
'36000', '55300', '84250', '46736', '85311', '38323', '47784', '25999', '10910',
'45205', '49200', '25720', '46231', '85421', '77220', '86220', '46769', '25300',
'46772', '66199', '43343', '46180', '46442', '49420', '33120', '91030', '47230',
'96012', '84120', '84130', '77399', '85531', '46710', '45206', '10820', '47786',
'47789', '46216', '16230', '66191', '46389', '47593', '47299', '52100', '46610',
'93130', '46321', '46732', '84111', '91011', '53200', '46349', '25110', '47770',
'25120', '66220', '46431', '47410', '46460', '81210', '47241', '73110', '79110',
'81220', '47521', '47591', '78200', '62010', '43341', '47114', '43910', '45113',
'56301', '96021'],
['03210', '77393', '93125', '88912', '01160', '01640', '08920', '85102',
'06100', '46214', '01700', '01210', '23490', '46215', '08123', '77296', '23910',
'13990', '86109', '27200', '33190', '87302', '88104', '08990', '30110', '46734',
'61300', '59202', '01630', '94920', '27330', '08121', '31099', '28296', '11060',
'09900', '15110', '32124', '95220', '46422', '01420', '42990', '23140', '46692',
'25920', '86103', '28293', '64991', '26520', '20120', '88996', '46495', '64110',
'55201', '46332', '20200', '32130', '46232', '25400', '46110', '28291', '24430',
'20520', '47594', '66290', '23200', '47524', '42219', '23110', '43342', '24540',
'22110', '62030', '46419', '86902', '20412', '01472', '32910', '32400', '26200',
'66120', '46311', '12000', '47222', '20130', '85204', '25991', '38222', '30300',
'15120', '65121', '17120', '23990', '20411', '77299', '87902', '31030', '17290',
'84232', '28940', '10311', '47251', '50200', '26300', '74209', '29100', '58290',
'77120', '46120', '47525', '52230', '28990', '24100', '59111', '90011', '69102',
'08112', '08111', '93123', '46471', '26110', '43333', '90041', '10830', '95110',
'74109', '46370', '86230', '46739', '47599', '74103', '74101', '74909', '28930',
'46170', '22210', '47890', '96032', '85313', '93199', '59120', '43996', '13100',
'46381', '38110', '47910', '95230', '52241', '74300', '47791', '68100', '95290',
'84119', '41102', '84241', '46220', '65122', '71112', '47430', '90012', '46150',
'52220', '47242', '84231', '43299', '46160', '68202', '77110', '47610', '28250',
'25501', '28220', '02200', '46620', '47722', '10130', '71122', '33110', '47740',
'47420', '31020', '49310', '81100', '82190', '23610', '43994', '70210', '75000',
'86101', '47530', '47781', '52210', '68203', '88995', '47713', '73200', '43390',
'71209', '41101', '01191', '46319', '38219', '01500', '96040', '47113', '78100',
'01130', '47512', '47592', '49320', '42110', '96022', '43221', '71111', '93110',
'47540', '47761', '62020', '69201', '64200', '47112', '81300', '47221', '64190',
'43320', '56102']]

# Begin big loop over radiuses (when spec_distance = False, else just use NACE's
# radius)

for radius in radiuses:

    # if we use a constant radius, we can calculate these maps once for all NACE
    if not spec_distance:
        nsize = radius
        sigma = stddevs[0]
        print "Working with size: %i" % nsize
        single_nsize = True
        if gauss:
            gscript.run_command('r.neighbors', input_='empl_total', size=nsize,
                            method='sum', gauss=sigma, output_=neigh_map_all, flags='c', 
                            quiet=True, overwrite=True)
            if use_income:
                gscript.run_command('r.neighbors', input_=total_income_map, 
                                size=nsize, method='sum', gauss=sigma, output_=neigh_map_inc, 
                                flags='c', quiet=True, overwrite=True)
        else:
            gscript.run_command('r.neighbors', input_='empl_total', size=nsize,
                            method='sum', output_=neigh_map_all, flags='c', 
                            quiet=True, overwrite=True)
            if use_income:
                gscript.run_command('r.neighbors', input_=total_income_map, 
                                size=nsize, method='sum', output_=neigh_map_inc, 
                                flags='c', quiet=True, overwrite=True)
    else:
        nsize = 0

    # If this is not a testrun, choose relevant group of NACE codes
    if run:
        rungroup = group[run-1]

    # Contstruct output file name with info about parameter choices
    exp_by_spatial_unit_and_nace_file = "exp_by_spatial_unit_and_nace"
    if spec_distance:
        exp_by_spatial_unit_and_nace_file += "_specdistance"
    if use_income:
        exp_by_spatial_unit_and_nace_file += "_inc"
    if gauss:
        exp_by_spatial_unit_and_nace_file += "_gauss"
    if run:
        exp_by_spatial_unit_and_nace_file += "_run%i" % run
    else:
        exp_by_spatial_unit_and_nace_file += "_testrun" 
    exp_by_spatial_unit_and_nace_file += "_nsize%03d.csv" % nsize 
        

    pop_by_spatial_unit_and_nace = {}

    # Main body of code, creating maps for each NACE
    for nace in rungroup:

        # Find the map of employment by pixel in that NACE
        empl_map = 'empl_nace%s' % nace
        res = gscript.find_file(empl_map, element='cell', mapset='empl_maps')
        if not res['name']:
            print "No empl_map found for NACE: %s" % nace
            continue

        print "Treating NACE %s " % nace
        avg_exp_value_nace = {}

        # If we use a different distance for each NACE, find the distance and
        # stddev values in meters and transform them to nsize and sigma in pixels
        if spec_distance:
            try:
                distance = distances[nace]
                stddev = stddevs[nace]
            except:
                print "No distance value for NACE: %s" % nace
                continue
            # size of search area and sigma for gaussian filter are defined by number of pixels
            # size has to be uneven
            nsize = int(math.ceil(distance / resolution))
            if nsize % 2 == 0:
                nsize += 1
            sigma = int(math.ceil(stddev / resolution))

        # Construct output map names with parameter choices
        rel_exp_nace = 'rel_export'
        exp_nace = 'export'
        if use_income:
            rel_exp_nace += "_inc"
            exp_nace += "_inc"
        rel_exp_nace += '_%s_%03d' % (nace, nsize)
        exp_nace += '_%s_%03d' % (nace, nsize)

        # find total number of employees within search radius in the current NACE
        # sector and for all sectors combined, using either gaussian or flat kernel
        if gauss:
            gscript.run_command('r.neighbors', input_=empl_map, size=nsize,
                    method='sum', gauss=sigma, output_=neigh_map_nace, flags='c',
                    quiet=True, overwrite=True)

            # The following are only necessary if nsize varies with NACE
            if spec_distance:
                gscript.run_command('r.neighbors', input_='empl_total', size=nsize,
                                    method='sum', gauss=sigma, output_=neigh_map_all, flags='c',
                                    quiet=True, overwrite=True)
                if use_income:
                    gscript.run_command('r.neighbors', input_=total_income_map, 
                                         size=nsize, method='sum', gauss=sigma, 
                                         output_=neigh_map_inc, flags='c',
                                         quiet=True, overwrite=True)

        else:
            gscript.run_command('r.neighbors', input_=empl_map, size=nsize,
                    method='sum', output_=neigh_map_nace, flags='c', 
                    quiet=True, overwrite=True)

            # The following are only necessary if nsize varies with NACE
            if spec_distance:
                gscript.run_command('r.neighbors', input_='empl_total', size=nsize,
                                    method='sum', output_=neigh_map_all, flags='c', 
                                    quiet=True, overwrite=True)
                if use_income:
                    gscript.run_command('r.neighbors', input_=total_income_map, 
                                        size=nsize, method='sum', output_=neigh_map_inc, 
                                        flags='c', quiet=True, overwrite=True)

        total_employment_stats_nace = gscript.parse_command('r.univar',
                                                         flags = "g",
                                                         map = empl_map)
        total_empl_nace = float(total_employment_stats_nace['sum'])


        nace2 = nace[:2]
        trade_balance = trade_balance_over_output_by_nace[nace2]

    # Estimating employment dedicated to exports taking into account 
    # national trade balance and, possibly, income instead of employment
    # Estimation based on LQ of NACE in a radius around a given location,
    # applied to the local employment in the specific NACE
        if use_income:
            mapcalc_expression = "%s = (( (%s / %f) - ((%s / %f) * (1 - %f))) * %f ) / %s" % (rel_exp_nace, 
                    neigh_map_nace, total_empl_nace, neigh_map_inc, total_income, trade_balance, total_empl_nace,
                    neigh_map_nace)
        else:
            mapcalc_expression = "%s = (( (%s / %f) - ((%s / %f) * (1 - %f))) * %f ) / %s" % (rel_exp_nace, 
                    neigh_map_nace, total_empl_nace, neigh_map_all, total_empl, trade_balance, total_empl_nace,
                    neigh_map_nace)


        gscript.run_command('r.mapcalc',
                       expression = mapcalc_expression,
                       overwrite = True,
                       quiet=True)

        # Only estimate export employment, not import
        mapcalc_expression = "%s = if(%s > 0, %s * %s, 0)" % (exp_nace,
                rel_exp_nace, rel_exp_nace, empl_map)

        gscript.run_command('r.mapcalc',
                       expression = mapcalc_expression,
                       overwrite = True,
                       quiet=True)

        gscript.run_command('v.rast.stats', map=new_nuts_map, raster=exp_nace,
                column_prefix='ex', method='sum', flags='c', quiet=True)

        for (unit, value) in [line.split('|') for line in
                gscript.read_command('v.db.select', map=new_nuts_map,
                    columns='unit,ex_sum', flags='c', quiet=True).splitlines()]: 
                    
            avg_exp_value_nace[unit] = value 
            avg_exp_value[nace] = avg_exp_value_nace

    # end of NACE-loop

    f = open(exp_by_spatial_unit_and_nace_file, 'w')
    for nace in avg_exp_value.iterkeys():
        for spatial_unit, value in avg_exp_value[nace].iteritems():
            if value: 
                output_string = nace + ';' + spatial_unit + ';' + str(value) + '\n'
                f.write(output_string)
    f.close()

# end of nsizes-loop

gscript.run_command('r.mask', flags='r', quiet=True)
gscript.run_command('g.remove', type='vect', name=new_nuts_map, flags='f',
        quiet=True)
