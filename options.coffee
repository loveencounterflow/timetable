
############################################################################################################
njs_path                  = require 'path'
#...........................................................................................................
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'timetable/options'
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge


#-----------------------------------------------------------------------------------------------------------
try
  data_info = require 'timetable-data'
  data_home = ( require 'path' ).dirname require.resolve 'timetable-data'
catch error
  warn "unable to `require 'timetable-data'`"
  help "please install data package with `npm install 'timetable-data'`"
  help "see #{( require './package.json')[ 'homepage' ]} for details"
  warn "aborting"
  process.exit()



#-----------------------------------------------------------------------------------------------------------
module.exports = options =
  #.........................................................................................................
  # 'mode': 'dev'
  #.........................................................................................................
  'parser':
    'delimiter':          ','
    'skip_empty_lines':   yes
  #.........................................................................................................
  'levelup':
    'route':              njs_path.join __dirname, './gtfs-db'
    'new':
      'keyEncoding':        'utf-8'
      'valueEncoding':      'json'
  #.........................................................................................................
  'data':
    # 'limit':              12
    ### OBS GTFS types MUST be in correct order, as some records (like stop_times) depend on other
    records (trips, stops) to be present in registry. ###
    'gtfs-types':         "agency calendar_dates calendar routes transfers trips stop_times stops".split /\s+/
    'node-types':         "agency route station course tour halt".split /\s+/
    'info':               data_info
    'home':               data_home



