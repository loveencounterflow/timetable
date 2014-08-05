



############################################################################################################
# njs_util                  = require 'util'
njs_path                  = require 'path'
njs_fs                    = require 'fs'
#...........................................................................................................
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'timetable/datasource-infos'
log                       = TRM.get_logger 'plain',     badge
info                      = TRM.get_logger 'info',      badge
whisper                   = TRM.get_logger 'whisper',   badge
alert                     = TRM.get_logger 'alert',     badge
debug                     = TRM.get_logger 'debug',     badge
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge
urge                      = TRM.get_logger 'urge',      badge
echo                      = TRM.echo.bind TRM
rainbow                   = TRM.rainbow.bind TRM
#...........................................................................................................
options                   = require '../options'
datasources_home          = options[ 'data' ][ 'home' ]
#...........................................................................................................
glob                      = require 'glob'


#-----------------------------------------------------------------------------------------------------------
get_datasource_infos = ( home = datasources_home ) ->
  R       = {}
  matcher = njs_path.join home, '**/*.txt'
  matcher = njs_path.join home, '**/*.txt-[a-z]{4}'
  #.........................................................................................................
  info "collecting data sources from"
  info rpr matcher
  #.........................................................................................................
  routes = glob.sync matcher
  if routes.length is 0
    warn "unable to find any datasource files in this location"
    help "please install data package with `npm install 'timetable-data'`"
    help "see #{( require './package.json')[ 'homepage' ]} for details"
    warn "aborting"
    process.exit()
  #.........................................................................................................
  for route in routes
    filename = njs_path.basename route, njs_path.extname route
    continue unless filename in options[ 'data' ][ 'types' ]
    #.......................................................................................................
    collection_name   = njs_path.basename njs_path.dirname route
    collection        = R[ collection_name    ]?= {}
    whisper route
    #.......................................................................................................
    collection[ filename  ] = route
  #.........................................................................................................
  return R


############################################################################################################
module.exports = get_datasource_infos()


