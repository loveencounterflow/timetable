

############################################################################################################
# njs_util                  = require 'util'
# njs_path                  = require 'path'
njs_fs                    = require 'fs'
# njs_crypto                  = require 'crypto'
#...........................................................................................................
# BAP                       = require 'coffeenode-bitsnpieces'
TYPES                     = require 'coffeenode-types'
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'TIMETABLE/read-gtfs-data'
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
# T                         = require './TRANSFORMERS'
# as_transformer            = T.as_transformer.bind T
# options                   = require '../options'
# global_data_limit         = options[ 'data' ]?[ 'limit' ] ? Infinity
# datasource_infos          = ( require './get-datasource-infos' )()
# create_readstream         = require '../create-readstream'
# REGISTRY                  = require './REGISTRY'
# #...........................................................................................................
# ASYNC                     = require 'async'
# #...........................................................................................................
# ### https://github.com/wdavidw/node-csv-parse ###
# _new_csv_parser           = require 'csv-parse'
# new_csv_parser            = -> _new_csv_parser options[ 'parser' ]
### http://c2fo.github.io/fast-csv/index.html, https://github.com/C2FO/fast-csv ###
# S                           = require 'string'

#-----------------------------------------------------------------------------------------------------------
# ES                        = require 'event-stream'
# fs                        = require 'fs'
# transform_stream          = require 'stream-transform'
# log                       = console.log
# as_transformer            = ( method ) -> transform_stream method, parallel: 11
# _new_csv_parser           = require 'csv-parse'
# new_csv_parser            = -> _new_csv_parser delimiter: ','
# $                         = ES.map.bind ES
P                         = require 'pipedreams'


#-----------------------------------------------------------------------------------------------------------
@$count = ( input_stream, title ) ->
  count = 0
  #.........................................................................................................
  input_stream.on 'end', ->
    info ( title ? 'Count' ) + ':', count
  #.........................................................................................................
  return $ ( record, handler ) =>
    count += 1
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$parse_csv = ->
  field_names = null
  return $ ( record, handler ) =>
    values = ( S record ).parseCSV ',', '"', '\\'
    if field_names is null
      field_names = values
      return handler()
    record = {}
    record[ field_names[ idx ] ] = value for value, idx in values
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$skip_empty = ->
  return $ ( record, handler ) =>
    return handler() if record.length is 0
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show = ->
  return $ ( record, handler ) =>
    urge rpr record
    # urge rpr record[ record.length - 1 ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@read_trips = ( route, handler ) ->
  input       = P.create_readstream route
  #.........................................................................................................
  input.on 'end', ->
    log 'ok: trips'
    return handler null
  input.setMaxListeners 100 # <<<<<<
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe P.$count    input, 'trips A'
    # .pipe P.$count    input, 'trips B'
    # .pipe P.$count    input, 'trips C'
    # .pipe P.$count    input, 'trips D'
    # .pipe P.$count    input, 'trips E'
    # .pipe P.$count    input, 'trips F'
    # .pipe P.$count    input, 'trips G'
    # .pipe P.$count    input, 'trips H'
    # .pipe P.$count    input, 'trips I'
    # .pipe P.$count    input, 'trips J'
    # .pipe P.$count    input, 'trips K'
    # .pipe P.$count    input, 'trips M'
    # .pipe P.$count    input, 'trips N'
    # .pipe P.$count    input, 'trips O'
    # .pipe P.$count    input, 'trips P'
    # .pipe P.$count    input, 'trips Q'
    # .pipe P.$count    input, 'trips R'
    # .pipe P.$count    input, 'trips R'
    # .pipe P.$count    input, 'trips S'
    # .pipe P.$count    input, 'trips T'
    # .pipe P.$count    input, 'trips U'
    # .pipe P.$count    input, 'trips V'
    # .pipe P.$count    input, 'trips W'
    # .pipe P.$count    input, 'trips X'
    # .pipe P.$count    input, 'trips Y'
    # .pipe P.$count    input, 'trips Z'
    # .pipe P.$show()
    .pipe P.$show_table input
  #.........................................................................................................
  return null

############################################################################################################
unless module.parent
  # route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/trips.txt'
  # route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/calendar.txt'
  # route = __filename
  route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/agency.txt'
  @read_trips route, ( error ) ->
    throw error if error?
    log 'ok'





