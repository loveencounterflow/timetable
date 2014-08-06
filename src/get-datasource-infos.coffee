



############################################################################################################
# njs_util                  = require 'util'
njs_path                  = require 'path'
njs_fs                    = require 'fs'
#...........................................................................................................
TYPES                     = require 'coffeenode-types'
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
module.exports = get_datasource_infos = ( home = datasources_home ) ->
  R       = {}
  matcher = njs_path.join home, '**/*.txt'
  count   = 0
  #.........................................................................................................
  info "collecting data sources from"
  info rpr matcher
  #.........................................................................................................
  for route in glob.sync matcher
    filename = njs_path.basename route, njs_path.extname route
    continue unless filename in options[ 'data' ][ 'gtfs-types' ]
    #.......................................................................................................
    count            += 1
    collection_name   = njs_path.basename njs_path.dirname route
    collection        = R[ collection_name    ]?= {}
    #.......................................................................................................
    collection[ filename  ] = route
  #.........................................................................................................
  matcher = njs_path.join home, '**/*.txt.-[a-z][a-z][a-z][a-z]'
  #.........................................................................................................
  for route in glob.sync matcher
    virtual_route = route.replace /\.-[a-z][a-z][a-z][a-z]$/, ''
    filename = njs_path.basename virtual_route, njs_path.extname virtual_route
    continue unless filename in options[ 'data' ][ 'gtfs-types' ]
    #.......................................................................................................
    count            += 1
    collection_name   = njs_path.basename njs_path.dirname virtual_route
    collection        = R[ collection_name    ]?= {}
    #.......................................................................................................
    target            = collection[ filename  ]?= []
    unless TYPES.isa_list target
      warn "found duplicate datasources for GTFS type #{rpr filename}"
      warn "in collection #{collection_name}:"
      warn "(1)", target
      warn "(2)", route
      help "when splitting large data files (recommended), please make sure"
      help "to delete the unsplitted file."
      warn "aborting"
      process.exit()
    #.......................................................................................................
    target.push route
  #.........................................................................................................
  if count is 0
    warn "unable to find any datasource files in this location"
    help "please install data package with `npm install 'timetable-data'`"
    help "see #{( require '../package.json')[ 'homepage' ]} for details"
    warn "aborting"
    process.exit()
  #.........................................................................................................
  return R


