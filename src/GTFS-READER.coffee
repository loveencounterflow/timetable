

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
badge                     = 'TIMETABLE/GTFS-READER'
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
global_data_limit         = options[ 'data' ]?[ 'limit' ] ? Infinity
datasource_infos          = ( require './get-datasource-infos' )()
REGISTRY                  = require './REGISTRY'
#...........................................................................................................
ASYNC                     = require 'async'
#...........................................................................................................
### https://github.com/loveencounterflow/pipedreams ###
P                         = require 'pipedreams'
$                         = P.$.bind P
#...........................................................................................................
DEV                       = options[ 'mode' ] is 'dev'


############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@$register = ( registry ) ->
  return $ ( record, handler ) =>
    REGISTRY.register_gtfs registry, record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$convert_latlon = ->
  return $ ( record, handler ) =>
    record[ 'lat' ] = parseFloat record[ 'lat' ]
    record[ 'lon' ] = parseFloat record[ 'lon' ]
    handler null, record



############################################################################################################
# SPECIFIC METHODS
#===========================================================================================================
# SPECIFIC METHODS: AGENCY
#-----------------------------------------------------------------------------------------------------------
@read_agency = ( registry, route, handler ) ->
  help 'read_agency'
  input       = P.create_readstream route, 'agency'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    # .pipe P.$sample                     1 / 10, headers: true, seed: 5
    .pipe P.$parse_csv()
    .pipe @$clean_agency_record()
    .pipe P.$delete_prefix              'agency_'
    .pipe P.$set                        '%gtfs-type', 'agency'
    .pipe P.$rename                     'id', '%gtfs-id'
    .pipe @$clean_agency_record()
    .pipe P.$dasherize_field_names()
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'agency', sample
    .on 'end', =>
      info 'ok: agency'
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_agency_record = ->
  return $ ( record, handler ) =>
    delete record[ 'agency_phone' ]
    delete record[ 'agency_lang' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: ROUTES
#-----------------------------------------------------------------------------------------------------------
### TAINT name clash (filesystem route vs. GTFS route) ###
@read_routes = ( registry, route, handler ) ->
  help 'read_routes'
  input       = P.create_readstream route, 'routes'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    # .pipe P.$sample                     1 / 100, headers: true, seed: 5
    .pipe P.$parse_csv()
    .pipe @$filter_routes               registry
    .pipe @$clean_routes_record()
    .pipe P.$dasherize_field_names()
    .pipe P.$set                        '%gtfs-type',       'routes'
    .pipe P.$rename                     'route-id',         '%gtfs-id'
    .pipe P.$rename                     'agency-id',        '%gtfs-agency-id'
    .pipe P.$rename                     'route-short-name', 'name'
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'route', sample
    .on 'end', =>
      info 'ok: routes'
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$filter_routes = ( registry ) ->
  return $ ( record, handler ) =>
    ### TAINT non-general filter ###
    matcher = if DEV then /^U4/ else /U/
    return handler null unless matcher.test record[ 'route_short_name' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$clean_routes_record = ->
  return $ ( record, handler ) =>
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
# SPECIFIC METHODS: CALENDAR_DATES
#-----------------------------------------------------------------------------------------------------------
@read_calendar_dates = ( registry, route, handler ) ->
  help 'read_calendar_dates'
  input       = P.create_readstream route, 'calendar_dates'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe @$filter_calendar_dates       registry
    # .pipe P.$sample                     1 / 100, headers: true, seed: 5
    .pipe @$clean_calendar_date_record()
    .pipe P.$set                        '%gtfs-type', 'calendar_dates'
    .pipe P.$rename                     'service_id', '%gtfs-id'
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'service', sample
    .on 'end', =>
      info 'ok: calendar_dates'
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$filter_calendar_dates = ( registry ) ->
  return $ ( record, handler ) =>
    ### TAINT non-general filter ###
    return handler null unless record[ 'date' ] is '20140624'
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$clean_calendar_date_record = ->
  return $ ( record, handler ) =>
    delete record[ 'exception_type' ]
    handler null, record



#===========================================================================================================
# SPECIFIC METHODS: TRIPS
#-----------------------------------------------------------------------------------------------------------
@read_trips = ( registry, route, handler ) ->
  help 'read_trips'
  input       = P.create_readstream route, 'trips'
  ratio       = if DEV then 1 / 100 else 1
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe @$filter_trips                registry
    .pipe P.$sample                     ratio, headers: true, seed: 5
    .pipe @$clean_trip_record()
    .pipe P.$delete_prefix              'trip_'
    .pipe P.$dasherize_field_names()
    .pipe P.$set                        '%gtfs-type', 'trips'
    .pipe P.$rename                     'id',         '%gtfs-id'
    .pipe P.$rename                     'route-id',   '%gtfs-routes-id'
    .pipe P.$rename                     'service-id', '%gtfs-service-id'
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'trip', sample
    .on 'end', =>
      info 'ok: trips'
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$filter_trips = ( registry ) ->
  return $ ( record, handler ) =>
    # return handler null unless registry[ '%gtfs' ][ 'trips'           ][ record[ 'trip_id'    ] ]?
    return handler null unless registry[ '%gtfs' ][ 'routes'          ][ record[ 'route_id'   ] ]?
    return handler null unless registry[ '%gtfs' ][ 'calendar_dates'  ][ record[ 'service_id' ] ]?
    handler null, record


#-----------------------------------------------------------------------------------------------------------
@$clean_trip_record = ->
  return $ ( record, handler ) =>
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
  help 'read_stop_times'
  input = P.create_readstream route, 'stop_times'
  ratio = if DEV then 1 / 1e4 else 1
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$sample                     ratio, headers: true
    .pipe P.$parse_csv()
    .pipe @$filter_stop_times           registry
    .pipe @$clean_stop_times_record()
    .pipe P.$set                        '%gtfs-type', 'stop_times'
    .pipe P.$dasherize_field_names()
    .pipe P.$rename                     'trip-id',        '%gtfs-trip-id'
    .pipe P.$rename                     'stop-id',        '%gtfs-stop-id'
    .pipe P.$rename                     'arrival-time',   'arr'
    .pipe P.$rename                     'departure-time', 'dep'
    .pipe @$add_stoptimes_gtfsid()
    .pipe @$register                    registry
    .pipe @$register_stop_id            registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'stop_time', sample
    .on 'end', =>
      info 'ok: stoptimes'
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_stop_times_record = ->
  return $ ( record, handler ) =>
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
@$filter_stop_times = ( registry ) ->
  return $ ( record, handler ) =>
    return handler null, record if DEV
    return handler null unless registry[ '%gtfs' ][ 'trips' ][ record[ 'trip_id' ] ]?
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$add_stoptimes_gtfsid = ->
  idx = 0
  return $ ( record, handler ) =>
    record[ '%gtfs-id' ]  = "#{idx}"
    idx += 1
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$register_stop_id = ( registry ) ->
  target = registry[ '%state' ][ 'gtfs-stop-ids' ]?= {}
  return $ ( record, handler ) =>
    target[ record[ '%gtfs-stop-id' ] ] = 1
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: STOPS
#-----------------------------------------------------------------------------------------------------------
@read_stops = ( registry, route, handler ) ->
  help 'read_stops'
  input = P.create_readstream route, 'stops'
  ratio = if DEV then 1 / 100 else 1
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$sample                     ratio, headers: true, seed: 5 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    .pipe P.$parse_csv()
    .pipe @$filter_stops                registry
    .pipe @$clean_stops_record()
    .pipe P.$delete_prefix              'stop_'
    .pipe P.$set                        '%gtfs-type', 'stops'
    # .pipe P.$copy                       'name', '%gtfs-name'
    .pipe P.$rename                     'id', '%gtfs-id'
    .pipe @$convert_latlon()
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'stop', sample
    .on 'end', =>
      info 'ok: stops'
      @_clear_stops_id_cache registry
      return handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@_clear_stops_id_cache = ( registry ) ->
  delete registry[ '%state' ][ 'gtfs-stop-ids' ]
  return null

#-----------------------------------------------------------------------------------------------------------
@$filter_stops = ( registry ) ->
  target = registry[ '%state' ][ 'gtfs-stop-ids' ]
  throw new Error "stops should be read after stop_times" unless target?
  return $ ( record, handler ) =>
    return handler null, record if DEV
    return handler null unless target[ record[ 'stop_id' ] ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$clean_stops_record = ->
  return $ ( record, handler ) =>
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
# READ METHOD
#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  t0 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  #.........................................................................................................
  for source_name, route_by_types of datasource_infos
    tasks     = []
    no_source = []
    no_method = []
    ok_types  = []
    #.......................................................................................................
    for gtfs_type in options[ 'data' ][ 'gtfs-types' ]
      if DEV
        if gtfs_type not in [ 'stop_times', 'stops', ]  # <<<<<<<<<<
          warn "skipping #{gtfs_type}"  # <<<<<<<<<<
          continue                    # <<<<<<<<<<
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
  ASYNC.series tasks, ( error ) =>
    throw error if error?
    t1 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    urge 'dt:', ( t1 - t0 ) / 1000 # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    handler null, registry
  #.........................................................................................................
  return null




############################################################################################################
# HELPERS
#===========================================================================================================

############################################################################################################
unless module.parent?
  registry = REGISTRY.new_registry()
  @main registry, ( error, registry ) ->
    throw error if error?
    info registry




