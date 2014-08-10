

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
GTFS_READER               = require './GTFS-READER'
READER                    = require './READER'
REGISTRY                  = require './REGISTRY'
options                   = require '../options'


#-----------------------------------------------------------------------------------------------------------
@main = ( handler ) ->
  registry = REGISTRY.new_registry()
  GTFS_READER.main registry, ( error ) =>
    return handler error if error?
    READER.main registry, ( error ) =>
      return handler error if error?
      handler null, registry
  #.........................................................................................................
  return null


############################################################################################################
unless module.parent?
  @main ( error, registry ) =>
    TEXT = require 'coffeenode-text'
    throw error if error?
    #.......................................................................................................
    for gtfs_type in options[ 'data' ][ 'gtfs-types' ]
      prefix = 'GTFS ' + ( TEXT.flush_left gtfs_type + ':', 15 )
      info prefix, ( Object.keys registry[ '%gtfs' ][ gtfs_type ] ).length
    #.......................................................................................................
    for type in options[ 'data' ][ 'timetable-types' ]
      prefix = '     ' + ( TEXT.flush_left type + ':', 15 )
      info prefix, ( Object.keys registry[ type ] ).length
    #.......................................................................................................
    setImmediate -> process.exit()


