

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
### TAINT very Berlin-specific method, shouldnt appear here ###
@_normalize_name = ( name ) ->
  name = name.replace /\s+\(Berlin\)(\s+Bus)?$/,      ''
  name = name.replace /^(U|S\+U|S)\s+/,               ''
  name = name.replace /^(Alexanderplatz) Bhf\/(.+)$/, '$1 ($2)'
  name = name.replace /^(Lichtenberg) Bhf\/(.+)$/,    '$1 ($2)'
  name = name.replace /^(Alexanderplatz) Bhf/,        '$1'
  name = name.replace /^(Zoologischer Garten) Bhf/,   '$1'
  name = name.replace /^(Gesundbrunnen) Bhf/,         '$1'
  name = name.replace /^(Potsdamer Platz) Bhf/,       '$1'
  name = name.replace /^(Lichtenberg) Bhf/,           '$1'
  name = name.replace /^(Friedrichstr\.) Bhf/,        '$1'
  name = name.replace /^(Jungfernheide) Bhf/,         '$1'
  name = name.replace /^(Stadtmitte) U[26]/,          '$1'
  name = name.replace /^(.+)str\./,                   '$1straße'
  name = name.replace /^(.+)\s+Str\./,                '$1 Straße'
  return name

#-----------------------------------------------------------------------------------------------------------
@_get_system_name = ( name ) ->
  name = name.toLowerCase()
  name = name.replace /,/g,  ''
  name = name.replace /\./g, ''
  name = name.replace /\s+/g, '-'
  return name

#-----------------------------------------------------------------------------------------------------------
### TAINT unify with following ###
@$normalize_station_name = ->
  return as_transformer ( record, handler ) =>
    record[ 'name' ] = @_normalize_name record[ 'name' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$normalize_headsign = ->
  return as_transformer ( record, handler ) =>
    record[ 'headsign' ] = @_normalize_name record[ 'headsign' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$convert_latlon = ->
  return as_transformer ( record, handler ) =>
    record[ 'lat' ] = parseFloat record[ 'lat' ]
    record[ 'lon' ] = parseFloat record[ 'lon' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$register = ( registry ) ->
  return as_transformer ( record, handler ) =>
    REGISTRY.register_gtfs registry, record
    return record


############################################################################################################
# SPECIFIC METHODS
#===========================================================================================================
# SPECIFIC METHODS: AGENCIES
#-----------------------------------------------------------------------------------------------------------
@$clean_agency_record = ->
  return as_transformer ( record, handler ) =>
    delete record[ 'agency_phone' ]
    delete record[ 'agency_lang' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: STOPTIMES
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
@$add_stoptime_idx = ->
  return as_transformer ( record, handler ) =>
    record[ 'idx' ] = ( parseInt record[ 'stop-sequence' ], 10 ) - 1
    delete record[ 'stop-sequence' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: ROUTES
#-----------------------------------------------------------------------------------------------------------
@$clean_route_record = ->
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
# SPECIFIC METHODS: STATIONS
#-----------------------------------------------------------------------------------------------------------
@$clean_station_record = ->
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

#-----------------------------------------------------------------------------------------------------------
@$add_station_system_name = ->
  return as_transformer ( record, handler ) =>
    record[ '~name' ] = @_get_system_name record[ 'name' ]
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: TRIPS
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

#-----------------------------------------------------------------------------------------------------------
@$add_headsign_system_name = ->
  return as_transformer ( record, handler ) =>
    record[ '~headsign' ] = @_get_system_name record[ 'headsign' ]
    handler null, record

############################################################################################################
# FINALIZATION
#-----------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------
@$add_agency_ids = ( registry ) ->
  return ( record ) ->
    record[ 'id'  ] = record[ '%gtfs-id' ].replace /[-_]+$/, ''
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_stoptime_ids = ( registry ) ->
  return ( record ) ->
    gtfs_stop_id    = record[ '%gtfs-stop-id' ]
    gtfs_trip_id    = record[ '%gtfs-trip-id' ]
    idx             = record[ 'idx' ]
    record[ 'id'  ] = "gtfs-stop:#{gtfs_stop_id}/gtfs-trip:#{gtfs_trip_id}/idx:#{idx}"
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_route_ids = ( registry ) ->
  route_idx     = -1
  return ( record ) ->
    route_idx      += 1
    gtfs_agency_id  = record[ '%gtfs-agency-id' ]
    gtfs_id         = record[ '%gtfs-id' ]
    name            = record[ 'name' ]
    agency          = registry[ 'old' ][ gtfs_agency_id ]
    return handler new Error "unable to find agency with GTFS ID #{rpr gtfs_agency_id}" unless agency?
    agency_id       = agency[ 'id' ]
    record[ 'id' ]  = "route:#{route_idx}/#{agency_id}/name:#{name}"
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_station_ids = ( registry ) ->
  return ( record ) ->
    stations_by_names = registry[ '%stations-by-names' ]?= {}
    sys_name          = record[ '~name' ]
    stations          = stations_by_names[ sys_name ]?= []
    station_idx       = stations.length
    stations.push record
    record[ 'id' ]    = "station/name:#{sys_name}/idx:#{station_idx}"
    # whisper '©4p1', record[ 'id'  ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_trip_ids = ( registry ) ->
  return ( record ) ->
    gtfs_trip_id      = record[ '%gtfs-id' ]
    gtfs_route_id     = record[ '%gtfs-route-id' ]
    # route             = registry[ 'old' ][ gtfs_route_id ]
    # sys_headsign      = record[ '~headsign' ]
    # record[ 'id' ]    = "station/headsign:#{sys_headsign}/gtfs-route-id:#{gtfs_route_id}/gtfs-trip-id:#{gtfs_trip_id}"
    ### does this make sense?? ###
    record[ 'id' ]    = "gtfs-route-id:#{gtfs_route_id}/gtfs-trip-id:#{gtfs_trip_id}"
    # whisper record
    # whisper '©4p1', record[ 'id'  ]
    return record

#-----------------------------------------------------------------------------------------------------------
@finalize = ( registry, handler ) ->
  method_by_types =
    'agency':     ( @$add_agency_ids    registry ).bind @
    'stoptime':   ( @$add_stoptime_ids  registry ).bind @
    'route':      ( @$add_route_ids     registry ).bind @
    'station':    ( @$add_station_ids   registry ).bind @
    'trip':       ( @$add_trip_ids      registry ).bind @
  for _, record of registry[ 'old' ]
    method = method_by_types[ label = record[ '~label' ] ]
    unless method?
      warn "unable to locate method `add_#{label}_ids; skipping"
      continue
    method record
    id = record[ 'id' ]
    unless id?
      warn "unable to find ID in #{record}; skipping"
      continue
    if ( duplicate = registry[ 'new' ][ id ] )?
      return handler new Error """
        duplicate IDs:
        #{rpr duplicate}
        #{rpr record}"""
    registry[ 'new' ][ id ] = record
  handler null


############################################################################################################
# MAKE IT SO
#-----------------------------------------------------------------------------------------------------------
@read_agencies = ( route, registry, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'agencies'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: agencies'
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
    .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS agencies...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_stop_times = ( route, registry, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'stop_times'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stoptimes'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$skip                       global_data_limit
    .pipe T.$as_pods()
    .pipe @$clean_stoptime_record()
    # .pipe @$fix_ids()
    .pipe T.$delete_prefix              'trip_'
    .pipe T.$add_n4j_system_properties  'node', 'stoptime'
    .pipe T.$dasherize_field_names()
    .pipe T.$rename                     'id',       '%gtfs-trip-id'
    .pipe T.$rename                     'stop-id',  '%gtfs-stop-id'
    .pipe @$add_stoptime_idx()
    # .pipe T.$register                   registry
    .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS stoptimes...'
  return null

#-----------------------------------------------------------------------------------------------------------
### TAINT name clash (filesystem route vs. GTFS route) ###
@read_routes = ( route, registry, handler ) ->
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
    .pipe @$clean_route_record()
    # .pipe @$fix_ids()
    .pipe T.$dasherize_field_names()
    .pipe T.$rename                     'route-id',         '%gtfs-id'
    .pipe T.$rename                     'agency-id',        '%gtfs-agency-id'
    .pipe T.$rename                     'route-short-name', 'name'
    .pipe T.$add_n4j_system_properties  'node', 'route'
    # .pipe T.$register                   registry
    .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS routes...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_stops = ( route, registry, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'stops'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stops'
    return handler null
  #.........................................................................................................
  input.pipe parser
    # .pipe T.$skip                       global_data_limit
    .pipe T.$as_pods()
    .pipe @$clean_station_record()
    .pipe T.$delete_prefix              'stop_'
    .pipe T.$set                        '%gtfs-type', 'stops'
    # .pipe T.$copy                       'name', '%gtfs-name'
    # .pipe @$normalize_station_name()
    # .pipe @$add_station_system_name()
    .pipe T.$rename                     'id', '%gtfs-id'
    # .pipe T.$add_n4j_system_properties  'node', 'station'
    .pipe @$convert_latlon()
    .pipe @$register                    registry
    .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS stops...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_trips = ( route, registry, handler ) ->
  parser      = new_csv_parser()
  input       = create_readstream route, 'trips'
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: trips'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$skip                       global_data_limit
    .pipe T.$as_pods()
    .pipe @$clean_trip_record()
    # .pipe @$fix_ids()
    .pipe T.$delete_prefix              'trip_'
    .pipe T.$dasherize_field_names()
    .pipe T.$rename                     'id',         '%gtfs-id'
    .pipe T.$rename                     'route-id',   '%gtfs-route-id'
    .pipe T.$rename                     'service-id', '%gtfs-service-id'
    .pipe T.$copy                       'headsign',   '%gtfs-headsign'
    .pipe @$normalize_headsign()
    .pipe @$add_headsign_system_name()
    .pipe T.$add_n4j_system_properties  'node', 'trip'
    # .pipe T.$register                   registry
    .pipe T.$show_sample                input
    # .pipe T.$show_and_quit()
  #.........................................................................................................
  whisper 'reading GTFS trips...'
  return null


#===========================================================================================================
# READ METHOD
#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  #.........................................................................................................
  for source_name, route_by_types of datasource_infos
    tasks     = []
    no_source = []
    no_method = []
    ok_types  = []
    #.......................................................................................................
    for gtfs_type in options[ 'data' ][ 'gtfs-types' ]
      route = route_by_types[ gtfs_type ]
      unless route?
        no_source.push "skipping #{source_name}/#{gtfs_type} (no source file)"
        continue
      help "found data source for #{source_name}/#{gtfs_type}"
      #.....................................................................................................
      method = null
      switch gtfs_type
        when 'agency'         then method = @read_agencies
        # when 'calendar_dates' then method = @read_calendar_dates
        # when 'calendar'       then method = @read_calendar
        # when 'routes'         then method = @read_routes
        # # when 'stop_times'     then method = @read_stop_times
        when 'stops'          then method = @read_stops
        # when 'transfers'      then method = @read_transfers
        # when 'trips'          then method = @read_trips
      unless method?
        no_method.push "no method to read GTFS data of type #{rpr gtfs_type}; skipping"
        continue
      method = method.bind @
      ok_types.push gtfs_type
      #.....................................................................................................
      do ( method, route ) =>
        tasks.push ( async_handler ) => method route, registry, async_handler
    #.......................................................................................................
    for messages in [ no_source, no_method, ]
      for message in messages
        warn message
    #.......................................................................................................
    info "reading data for #{ok_types.length} type(s)"
    info "  (#{ok_types.join ', '})"
  #.........................................................................................................
  # limit = options[ 'stream-transform' ]?[ 'parallel' ] ? 1
  ASYNC.series tasks, ( error ) =>
    throw error if error?
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




