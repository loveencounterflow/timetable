

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
T                         = require './TRANSFORMERS'
as_transformer            = T.as_transformer.bind T
options                   = require '../options'
global_data_limit         = options[ 'data' ]?[ 'limit' ] ? Infinity
datasource_infos          = ( require './get-datasource-infos' )()
create_readstream         = require './create-readstream'
REGISTRY                  = require './REGISTRY'
#...........................................................................................................
ASYNC                     = require 'async'
#...........................................................................................................
### https://github.com/wdavidw/node-csv-parse ###
_new_csv_parser           = require 'csv-parse'
new_csv_parser            = -> _new_csv_parser options[ 'parser' ]



############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@$register = ( registry ) ->
  return as_transformer ( record, handler ) =>
    REGISTRY.register_gtfs registry, record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$convert_latlon = ->
  return as_transformer ( record, handler ) =>
    record[ 'lat' ] = parseFloat record[ 'lat' ]
    record[ 'lon' ] = parseFloat record[ 'lon' ]
    handler null, record


############################################################################################################
# SPECIFIC METHODS
#===========================================================================================================
# SPECIFIC METHODS: AGENCY
#-----------------------------------------------------------------------------------------------------------
@read_agency = ( registry, route, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'agency'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: agency'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_agency_record()
    .pipe T.$delete_prefix              'agency_'
    .pipe T.$set                        '%gtfs-type', 'agency'
    .pipe T.$rename                     'id', '%gtfs-id'
    .pipe T.$dasherize_field_names()
    .pipe @$register                    registry
    # .pipe T.$show()
    .pipe T.$count                      input, 'agency'
    # .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS agency...'
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_agency_record = ->
  return as_transformer ( record, handler ) =>
    delete record[ 'agency_phone' ]
    delete record[ 'agency_lang' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: ROUTES
#-----------------------------------------------------------------------------------------------------------
### TAINT name clash (filesystem route vs. GTFS route) ###
@read_routes = ( registry, route, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'routes'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: routes'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$skip                       global_data_limit
    .pipe T.$as_pods()
    .pipe @$clean_routes_record()
    .pipe T.$dasherize_field_names()
    .pipe T.$set                        '%gtfs-type',       'routes'
    .pipe T.$rename                     'route-id',         '%gtfs-id'
    .pipe T.$rename                     'agency-id',        '%gtfs-agency-id'
    .pipe T.$rename                     'route-short-name', 'name'
    .pipe @$register                    registry
    .pipe T.$count                      input, 'routes'
    # .pipe T.$show_sample                input
  #.........................................................................................................
  whisper 'reading GTFS routes...'
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_routes_record = ->
  return as_transformer ( record, handler ) =>
    # delete record[ 'route_id'         ]
    # delete record[ 'agency_id'        ]
    # delete record[ 'route_short_name' ]
    delete record[ 'route_long_name'  ]
    delete record[ 'route_desc'       ]
    # delete record[ 'route_type'       ]
    delete record[ 'route_url'        ]
    delete record[ 'route_color'      ]
    delete record[ 'route_text_color' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: STOPS
#-----------------------------------------------------------------------------------------------------------
@read_stops = ( registry, route, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'stops'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stops'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$skip                       global_data_limit
    .pipe T.$as_pods()
    .pipe @$clean_stops_record()
    .pipe T.$delete_prefix              'stop_'
    .pipe T.$set                        '%gtfs-type', 'stops'
    # .pipe T.$copy                       'name', '%gtfs-name'
    .pipe T.$rename                     'id', '%gtfs-id'
    .pipe @$convert_latlon()
    .pipe @$register                    registry
    .pipe T.$count                      input, 'stops'
    # .pipe T.$show_sample                input
  #.........................................................................................................
  whisper 'reading GTFS stops...'
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_stops_record = ->
  return as_transformer ( record, handler ) =>
    # delete record[ 'stop_id'        ]
    # delete record[ 'stop_code'      ]
    # delete record[ 'stop_name'      ]
    delete record[ 'stop_desc'      ]
    # delete record[ 'stop_lat'       ]
    # delete record[ 'stop_lon'       ]
    delete record[ 'zone_id'        ]
    delete record[ 'stop_url'       ]
    delete record[ 'location_type'  ]
    delete record[ 'parent_station' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: TRIPS
# #-----------------------------------------------------------------------------------------------------------
# @read_trips = ( registry, route, handler ) ->
#   parser      = new_csv_parser()
#   input       = create_readstream route, 'trips'
#   #.........................................................................................................
#   input.on 'end', ->
#     info 'ok: trips'
#     return handler null
#   #.........................................................................................................
#   input.pipe parser
#     .pipe T.$skip                       global_data_limit
#     .pipe T.$as_pods()
#     .pipe @$clean_trip_record()
#     .pipe T.$delete_prefix              'trip_'
#     .pipe T.$dasherize_field_names()
#     .pipe T.$set                        '%gtfs-type', 'trips'
#     .pipe T.$rename                     'id',         '%gtfs-id'
#     .pipe T.$rename                     'route-id',   '%gtfs-routes-id'
#     .pipe T.$rename                     'service-id', '%gtfs-service-id'
#     .pipe @$register                    registry
#     .pipe T.$show_sample                input
#   #.........................................................................................................
#   whisper 'reading GTFS trips...'
#   return null

#-----------------------------------------------------------------------------------------------------------
@read_trips = ( registry, route, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'trips'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: trips'
    return handler null
  input.setMaxListeners 100 # <<<<<<
  #.........................................................................................................
  # input.pipe parser
  input.pipe T.$count                   input, 'trips A'
    .pipe T.$count                      input, 'trips B'
    .pipe T.$count                      input, 'trips C'
    .pipe T.$count                      input, 'trips D'
    .pipe T.$count                      input, 'trips E'
    .pipe T.$count                      input, 'trips F'
    .pipe T.$count                      input, 'trips G'
    .pipe T.$count                      input, 'trips H'
    .pipe T.$count                      input, 'trips I'
    .pipe T.$count                      input, 'trips J'
    .pipe T.$count                      input, 'trips K'
    .pipe T.$count                      input, 'trips M'
    .pipe T.$count                      input, 'trips N'
    .pipe T.$count                      input, 'trips O'
    .pipe T.$count                      input, 'trips P'
    .pipe T.$count                      input, 'trips Q'
    .pipe T.$count                      input, 'trips R'
    .pipe T.$count                      input, 'trips R'
    .pipe T.$count                      input, 'trips S'
    .pipe T.$count                      input, 'trips T'
    .pipe T.$count                      input, 'trips U'
    .pipe T.$count                      input, 'trips V'
    .pipe T.$count                      input, 'trips W'
    .pipe T.$count                      input, 'trips X'
    .pipe T.$count                      input, 'trips Y'
    .pipe T.$count                      input, 'trips Z'
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_trip_record = ->
  return as_transformer ( record, handler ) =>
    # delete record[ 'route_id'        ]
    # delete record[ 'service_id'      ]
    # delete record[ 'trip_id'         ]
    # delete record[ 'trip_headsign'   ]
    delete record[ 'trip_short_name' ]
    delete record[ 'direction_id'    ]
    delete record[ 'block_id'        ]
    delete record[ 'shape_id'        ]
    handler null, record

#===========================================================================================================
# SPECIFIC METHODS: STOPTIMES
#-----------------------------------------------------------------------------------------------------------
@read_stop_times = ( registry, route, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'stop_times'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stoptimes'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$count                      input, 'stop_times A'
    .pipe T.$skip                       global_data_limit
    .pipe T.$count                      input, 'stop_times B'
    .pipe T.$as_pods()
    .pipe T.$count                      input, 'stop_times C'
    .pipe @$clean_stoptime_record()
    .pipe T.$count                      input, 'stop_times D'
    .pipe T.$set                        '%gtfs-type', 'stop_times'
    # .pipe T.$delete_prefix              'trip_'
    # .pipe T.$dasherize_field_names()
    # .pipe T.$rename                     'id',             '%gtfs-trip-id'
    # .pipe T.$rename                     'stop-id',        '%gtfs-stop-id'
    # .pipe T.$rename                     'arrival-time',   'arr'
    # .pipe T.$rename                     'departure-time', 'dep'
    # .pipe @$add_stoptimes_gtfsid()
    # .pipe @$register                    registry
    .pipe T.$count                      input, 'stop_times E'
    # .pipe T.$show_sample                input
  #.........................................................................................................
  whisper 'reading GTFS stoptimes...'
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_stoptime_record = ->
  return as_transformer ( record, handler ) =>
    # delete record[ 'trip_id'             ]
    # delete record[ 'arrival_time'        ]
    # delete record[ 'departure_time'      ]
    # delete record[ 'stop_id'             ]
    # delete record[ 'stop_sequence'       ]
    delete record[ 'stop_headsign'       ]
    delete record[ 'pickup_type'         ]
    delete record[ 'drop_off_type'       ]
    delete record[ 'shape_dist_traveled' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$add_stoptimes_gtfsid = ->
  return as_transformer ( record, handler ) =>
    idx += 1
    # record[ '%gtfs-id' ]  = "#{record[ '']}"
    handler null, record


#===========================================================================================================
# READ METHOD
#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  # t0 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  #.........................................................................................................
  for source_name, route_by_types of datasource_infos
    tasks     = []
    no_source = []
    no_method = []
    ok_types  = []
    #.......................................................................................................
    for gtfs_type in options[ 'data' ][ 'gtfs-types' ]
      if gtfs_type is 'stop_times'  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        warn "skipping stop_times"  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        continue                    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      route = route_by_types[ gtfs_type ]
      unless route?
        no_source.push "skipping #{source_name}/#{gtfs_type} (no source file)"
        continue
      help "found data source for #{source_name}/#{gtfs_type}"
      #.....................................................................................................
      method_name = "read_#{gtfs_type}"
      method      = @[ method_name ]
      unless method?
        no_method.push "no method to read GTFS data of type #{rpr gtfs_type}; skipping"
        continue
      method = method.bind @
      ok_types.push gtfs_type
      #.....................................................................................................
      do ( method, route ) =>
        tasks.push ( async_handler ) => method registry, route, async_handler
    #.......................................................................................................
    for messages in [ no_source, no_method, ]
      for message in messages
        warn message
    #.......................................................................................................
    info "reading data for #{ok_types.length} type(s)"
    info "  (#{ok_types.join ', '})"
  #.........................................................................................................
  limit = options[ 'stream-transform' ]?[ 'parallel' ] ? 1
  # ASYNC.parallelLimit tasks, limit, ( error ) =>
  ASYNC.series tasks, ( error ) =>
    throw error if error?
    # t1 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # urge 'dt:', ( t1 - t0 ) / 1000 # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    handler null, registry
  #.........................................................................................................
  return null




############################################################################################################
# HELPERS
#===========================================================================================================

############################################################################################################
unless module.parent?
  @main ( error, registry ) ->
    throw error if error?
    info registry




