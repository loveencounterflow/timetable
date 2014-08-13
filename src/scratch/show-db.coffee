

############################################################################################################
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'TIMETABLE/main'
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
GTFS_READER               = require '../GTFS-READER'
READER                    = require '../READER'
REGISTRY                  = require '../REGISTRY'
options                   = require '../../options'



#-----------------------------------------------------------------------------------------------------------
@show_db = ( db, handler ) ->
  P = require 'pipedreams'
  $ = P.$.bind P
  # options =
  #   gt:     'a'
  #   lt:     'z'
  help 'show registry'
  count = 0
  db_options =
    'gte':         'gtfs/agency'
    'lte':         'gtfs/agency/\xff'
    # 'gte':          'gtfs/stops'
    # 'lte':          'gtfs/stops/\xff'
    'keys':         yes
    'values':       no
  # input = ( db.createReadStream options[ 'levelup' ][ 'new' ] )
  whisper db_options
  input = ( db.createReadStream db_options )
    .pipe P.$show()
    .pipe $ ( record, handler ) ->
      count += 1
      handler null, record
    .pipe P.$on_end ->
      help count, "records in DB"
      return handler null, db


#-----------------------------------------------------------------------------------------------------------
@main = ( handler ) ->
  registry = REGISTRY.new_registry()
  @show_db registry, handler
  return null


############################################################################################################
unless module.parent?
  @main ( error, registry ) =>
    throw error if error?
    help 'ok'
