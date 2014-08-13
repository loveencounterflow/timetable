

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
ASYNC                     = require 'async'
#...........................................................................................................
GTFS_READER               = require './GTFS-READER'
READER                    = require './READER'
REGISTRY                  = require './REGISTRY'
options                   = require '../options'



#-----------------------------------------------------------------------------------------------------------
@show_db = ( db, handler ) ->
  P = require 'pipedreams'
  $ = P.$.bind P
  query =
    gte:     'gtfs/stops/'
    lte:     'gtfs/stops/\xff'
  help 'show registry'
  count = 0
  # input = ( db.createReadStream query[ 'levelup' ][ 'new' ] )
  input = ( db.createReadStream query )
    # .pipe P.$show()
    .pipe $ ( record, handler ) ->
      count += 1
      handler null, record
    .pipe P.$on_end ->
      help query
      help count, "records in DB"
      return handler null, db


#-----------------------------------------------------------------------------------------------------------
@main = ( handler ) ->
  tasks             = []
  [ folder_exists
    registry      ] = REGISTRY._new_registry()
  #.........................................................................................................
  if folder_exists
    help "DB already exists"
    help "skipping GTFS_READER.main()"
  else
    tasks.push ( async_handler ) ->
      GTFS_READER.main registry, async_handler
  #.........................................................................................................
  tasks.push ( async_handler ) -> REGISTRY.flush registry, async_handler
  # tasks.push ( async_handler ) -> READER.main registry, async_handler
  # tasks.push ( async_handler ) -> REGISTRY.flush registry, async_handler
  #.........................................................................................................
  ASYNC.series tasks, ( error ) ->
    return handler error if error?
    handler null, registry
  #.........................................................................................................
  return null


############################################################################################################
unless module.parent?
  @main ( error, registry ) =>
    throw error if error?
    @show_db registry, ( error ) ->
      throw error if error?
      help 'ok'



