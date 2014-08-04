

############################################################################################################
# njs_util                  = require 'util'
# njs_path                  = require 'path'
njs_fs                    = require 'fs'
# njs_crypto                  = require 'crypto'
#...........................................................................................................
# BAP                       = require 'coffeenode-bitsnpieces'
# TYPES                     = require 'coffeenode-types'
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'timetable/main'
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
### https://github.com/wdavidw/node-csv-parse ###
new_parser                = require 'csv-parse'
#...........................................................................................................
T                         = require './TRANSFORMERS'
as_transformer            = T.as_transformer.bind T
options                   = require '../options'



############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
### TAINT very Berlin-specific method, shouldnt appear here ###
@_normalize_name = ( name ) ->
  name = name.replace /\s+\(Berlin\)(\s+Bus)?$/, ''
  name = name.replace /^(U|S\+U)\s+/, ''
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
  return as_transformer ( record ) =>
    record[ 'name' ] = @_normalize_name record[ 'name' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$normalize_headsign = ->
  return as_transformer ( record ) =>
    record[ 'headsign' ] = @_normalize_name record[ 'headsign' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$convert_latlon = ->
  return as_transformer ( record ) =>
    record[ 'lat' ] = parseFloat record[ 'lat' ]
    record[ 'lon' ] = parseFloat record[ 'lon' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$fix_ids = ->
  return as_transformer ( record ) =>
    for name, value of record
      match = name.match /^(.+)_id$/
      continue unless match?
      [ ignore
        prefix ] = match
      record[ name ] = "#{prefix}-#{value}"
    return record

# #-----------------------------------------------------------------------------------------------------------
# @$whisper = ( message ) ->
#   return as_transformer ( record ) =>
#     whisper message
#     return record


############################################################################################################
# SPECIFIC METHODS
#===========================================================================================================
# SPECIFIC METHODS: AGENCIES
#-----------------------------------------------------------------------------------------------------------
@$clean_agency_record = ->
  return as_transformer ( record ) ->
    delete record[ 'agency_phone' ]
    delete record[ 'agency_lang' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_agency_id = ->
  return as_transformer ( record ) =>
    record[ 'id'  ] = record[ '%gtfs-id' ].replace /[-_]+$/, ''
    return record


#===========================================================================================================
# SPECIFIC METHODS: STOPTIMES
#-----------------------------------------------------------------------------------------------------------
@$clean_stoptime_record = ->
  return as_transformer ( record ) =>
    # delete record[ 'trip_id'             ]
    # delete record[ 'arrival_time'        ]
    # delete record[ 'departure_time'      ]
    # delete record[ 'stop_id'             ]
    # delete record[ 'stop_sequence'       ]
    delete record[ 'stop_headsign'       ]
    delete record[ 'pickup_type'         ]
    delete record[ 'drop_off_type'       ]
    delete record[ 'shape_dist_traveled' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_stoptime_idx = ->
  return as_transformer ( record ) =>
    record[ 'idx' ] = ( parseInt record[ 'stop-sequence' ], 10 ) - 1
    delete record[ 'stop-sequence' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_stoptime_id = ->
  return as_transformer ( record ) =>
    gtfs_stop_id    = record[ '%gtfs-stop-id' ]
    gtfs_trip_id    = record[ '%gtfs-trip-id' ]
    idx             = record[ 'idx' ]
    record[ 'id'  ] = "gtfs-stop:#{gtfs_stop_id}/gtfs-trip:#{gtfs_trip_id}/idx:#{idx}"
    return record


#===========================================================================================================
# SPECIFIC METHODS: ROUTES
#-----------------------------------------------------------------------------------------------------------
@$clean_route_record = ->
  return as_transformer ( record ) =>
    # delete record[ 'route_id'         ]
    # delete record[ 'agency_id'        ]
    # delete record[ 'route_short_name' ]
    delete record[ 'route_long_name'  ]
    delete record[ 'route_desc'       ]
    # delete record[ 'route_type'       ]
    delete record[ 'route_url'        ]
    delete record[ 'route_color'      ]
    delete record[ 'route_text_color' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_route_id = ( registry ) ->
  route_idx     = -1
  return as_transformer ( record ) =>
    route_idx      += 1
    gtfs_agency_id  = record[ '%gtfs-agency-id' ]
    gtfs_id         = record[ '%gtfs-id' ]
    name            = record[ 'name' ]
    agency          = registry[ 'old' ][ gtfs_agency_id ]
    return handler new Error "unable to find agency with GTFS ID #{rpr gtfs_agency_id}" unless agency?
    agency_id       = agency[ 'id' ]
    record[ 'id' ]  = "route:#{route_idx}/#{agency_id}/name:#{name}"
    return record


#===========================================================================================================
# SPECIFIC METHODS: STATIONS
#-----------------------------------------------------------------------------------------------------------
@$clean_station_record = ->
  return as_transformer ( record ) =>
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
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_station_system_name = ->
  return as_transformer ( record ) =>
    record[ '~name' ] = @_get_system_name record[ 'name' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_station_id = ( registry ) ->
  return as_transformer ( record ) =>
    stations_by_names = registry[ '%stations-by-names' ]?= {}
    sys_name          = record[ '~name' ]
    stations          = stations_by_names[ sys_name ]?= []
    station_idx       = stations.length
    stations.push record
    record[ 'id' ]    = "station/name:#{sys_name}/idx:#{station_idx}"
    # whisper '©4p1', record[ 'id'  ]
    return record


#===========================================================================================================
# SPECIFIC METHODS: TRIPS
#-----------------------------------------------------------------------------------------------------------
@$clean_trip_record = ->
  return as_transformer ( record ) =>
    # delete record[ 'route_id'        ]
    # delete record[ 'service_id'      ]
    # delete record[ 'trip_id'         ]
    # delete record[ 'trip_headsign'   ]
    delete record[ 'trip_short_name' ]
    delete record[ 'direction_id'    ]
    delete record[ 'block_id'        ]
    delete record[ 'shape_id'        ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_trip_headsign_system_name = ->
  return as_transformer ( record ) =>
    record[ '~headsign' ] = @_get_system_name record[ 'headsign' ]
    return record

#-----------------------------------------------------------------------------------------------------------
@$add_trip_id = ( registry ) ->
  return as_transformer ( record ) =>
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


############################################################################################################
# MAKE IT SO
#-----------------------------------------------------------------------------------------------------------
@read_agencies = ( registry, handler ) ->
  parser      = new_parser options[ 'parser' ]
  route       = '/Volumes/Storage/cnd/node_modules/timetable/data/germany-berlin-2014/agency.txt'
  input       = njs_fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: agencies'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_agency_record()
    .pipe @$fix_ids()
    .pipe T.$delete_prefix 'agency_'
    .pipe T.$add_n4j_system_properties 'node', 'agency'
    .pipe T.$rename 'id', '%gtfs-id'
    .pipe @$add_agency_id()
    .pipe T.$dasherize_field_names()
    .pipe T.$register registry
    # .pipe T.show_and_quit
  #.........................................................................................................
  whisper 'reading GTFS agencies...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_stoptimes = ( registry, handler ) ->
  parser      = new_parser options[ 'parser' ]
  ### TAINT must concatenate files or read both parts ###
  route       = '/Volumes/Storage/cnd/node_modules/timetable/data/germany-berlin-2014/stop_times.001.txt'
  input       = njs_fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stoptimes'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_stoptime_record()
    .pipe @$fix_ids()
    .pipe T.$delete_prefix 'trip_'
    .pipe T.$add_n4j_system_properties 'node', 'stoptime'
    .pipe T.$dasherize_field_names()
    .pipe T.$rename 'id',       '%gtfs-trip-id'
    .pipe T.$rename 'stop-id',  '%gtfs-stop-id'
    .pipe @$add_stoptime_idx()
    .pipe @$add_stoptime_id()
    .pipe T.$register registry
    # .pipe T.show_and_quit
  #.........................................................................................................
  whisper 'reading GTFS stoptimes...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_routes = ( registry, handler ) ->
  parser        = new_parser options[ 'parser' ]
  ### TAINT name clash (filesystem route vs. GTFS route) ###
  route         = '/Volumes/Storage/cnd/node_modules/timetable/data/germany-berlin-2014/routes.txt'
  input         = njs_fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: routes'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_route_record()
    .pipe @$fix_ids()
    .pipe T.$dasherize_field_names()
    .pipe T.$rename 'route-id',         '%gtfs-id'
    .pipe T.$rename 'agency-id',        '%gtfs-agency-id'
    .pipe T.$rename 'route-short-name', 'name'
    .pipe T.$add_n4j_system_properties 'node', 'route'
    .pipe @$add_route_id registry
    .pipe T.$register registry
    # .pipe T.show_and_quit
  #.........................................................................................................
  whisper 'reading GTFS routes...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_stations = ( registry, handler ) ->
  parser          = new_parser options[ 'parser' ]
  route           = '/Volumes/Storage/cnd/node_modules/timetable/data/germany-berlin-2014/stops.txt'
  input           = njs_fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: stations'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_station_record()
    .pipe @$fix_ids()
    .pipe T.$delete_prefix 'stop_'
    .pipe T.$copy 'name', '%gtfs-name'
    .pipe @$normalize_station_name()
    .pipe @$add_station_system_name()
    .pipe T.$rename 'id', '%gtfs-id'
    .pipe T.$add_n4j_system_properties 'node', 'station'
    .pipe @$convert_latlon()
    .pipe @$add_station_id registry
    .pipe T.$register registry
    # .pipe T.show_and_quit
  #.........................................................................................................
  whisper 'reading GTFS stations...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read_trips = ( registry, handler ) ->
  parser          = new_parser options[ 'parser' ]
  route           = '/Volumes/Storage/cnd/node_modules/timetable/data/germany-berlin-2014/trips.txt'
  input           = njs_fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    info 'ok: trips'
    return handler null
  #.........................................................................................................
  input.pipe parser
    .pipe T.$as_pods()
    .pipe @$clean_trip_record()
    .pipe @$fix_ids()
    .pipe T.$delete_prefix 'trip_'
    .pipe T.$dasherize_field_names()
    .pipe T.$rename 'id',         '%gtfs-id'
    .pipe T.$rename 'route-id',   '%gtfs-route-id'
    .pipe T.$rename 'service-id', '%gtfs-service-id'
    .pipe T.$copy   'headsign',   '%gtfs-headsign'
    .pipe @$normalize_headsign()
    .pipe @$add_trip_headsign_system_name()
    .pipe T.$add_n4j_system_properties 'node', 'trip'
    .pipe @$add_trip_id registry
    .pipe T.$register registry
    # .pipe T.show_and_quit
  #.........................................................................................................
  whisper 'reading GTFS trips...'
  return null

#-----------------------------------------------------------------------------------------------------------
@read = ( handler ) ->
  registry    = {}
  # @read_agencies   registry
  tasks       = [
    ( async_handler ) => @read_agencies   registry, async_handler
    ( async_handler ) => @read_stoptimes  registry, async_handler
    ( async_handler ) => @read_routes     registry, async_handler
    ( async_handler ) => @read_stations   registry, async_handler
    ( async_handler ) => @read_trips      registry, async_handler
    ]
  ASYNC.series tasks, ( error ) =>
    throw error if error?
    ro = registry[ 'old' ]
    if ro?
      count = ( Object.keys ro ).length
      debug "#{count} entries in registry[ 'old' ]"
    else
      debug "no entries in registry[ 'old' ]"
  #   # info registry[ 'new' ]
  #   # info ( Object.keys registry[ 'new' ] ).length if registry[ 'new' ]?
  #   # if ( stations_by_names = registry[ '%stations-by-names' ] )?
  #   #   # info ( Object.keys stations_by_names ).join ' '
  #   #   for stop_name, stops of stations_by_names
  #   #     # continue if stops.length < 2
  #   #     # continue unless /^alt-ma/.test stop_name
  #   #     continue unless /alt-mariendorf/.test stop_name
  #   #     log rainbow stops
  #   # info registry


############################################################################################################
unless module.parent?
  # @read()

  info require 'timetable-data'



