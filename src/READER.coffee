

############################################################################################################
# njs_util                  = require 'util'
# njs_path                  = require 'path'
# njs_fs                    = require 'fs'
# njs_crypto                  = require 'crypto'
#...........................................................................................................
# BAP                       = require 'coffeenode-bitsnpieces'
# TYPES                     = require 'coffeenode-types'
# TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'TIMETABLE/READER'
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
REGISTRY                  = require './REGISTRY'
COURSES                   = require './COURSES'
P                         = require 'pipedreams'
$                         = P.$.bind P
#...........................................................................................................
options                   = require '../options'
DEV                       = options[ 'mode' ] is 'dev'


#===========================================================================================================
#
#-----------------------------------------------------------------------------------------------------------
### TAINT very Berlin-specific method, shouldnt appear here ###
@normalize_berlin_station_name = ( name ) ->
  name = name.replace /\s+\(Berlin\)(\s+Bus)?$/,      ''
  unless /^(U|S) Olympiastadion/.test name
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
  name = name.replace /str\./g,                       'straße'
  name = name.replace /\s+Str\./g,                    ' Straße'
  return name

#-----------------------------------------------------------------------------------------------------------
@$normalize_station_name = ( key ) ->
  return $ ( node, handler ) =>
    name = node[ key ]
    unless name?
      return handler new Error """
        unable to find key #{rpr key} in
        #{rpr node}"""
    node[ key ] = @normalize_berlin_station_name name
    handler null, node

#-----------------------------------------------------------------------------------------------------------
@$add_n4j_system_properties = ( label ) ->
  return $ ( node, handler ) ->
    node[ '~isa'    ] = 'node'
    node[ '~label'  ] = label
    handler null, node

#-----------------------------------------------------------------------------------------------------------
@$remove_gtfs_fields = ->
  return $ ( node, handler ) ->
    for name of node
      delete node[ name ] if /^%gtfs/.test name
    handler null, node

#-----------------------------------------------------------------------------------------------------------
@$add_id = ->
  idx = 0
  return $ ( node, handler ) ->
    id            = node[ 'id'  ]
    label         = node[ '~label'  ]
    return handler new Error "ID already set: #{rpr node}"                         if        id?
    return handler new Error "unable to set ID on node without label: #{rpr node}" unless label?
    node[ 'id' ]  = "#{label}-#{idx}"
    idx          += 1
    handler null, node


#===========================================================================================================
# STATIONS
#-----------------------------------------------------------------------------------------------------------
@read_station_nodes = ( registry, handler ) ->
  # ratio = if DEV then 1 / 100 else 1
  input = registry.createReadStream keys: no, gte: 'gtfs/stops', lte: 'gtfs/stops\xff'
  #.........................................................................................................
  input
    # .pipe P.$sample                     ratio, headers: true, seed: 5 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    .pipe @$normalize_station_name      'name'
    .pipe @$add_n4j_system_properties   'station'
    .pipe @$add_id()
    .pipe @$remove_gtfs_fields()
    # .pipe REGISTRY.$register            registry
    .pipe P.$show()
    .pipe P.$collect_sample             1, ( _, sample ) -> whisper 'station', sample
    .pipe P.$on_end                     -> handler null


#===========================================================================================================
# ROUTES
#-----------------------------------------------------------------------------------------------------------
@read_route_nodes = ( registry, handler ) ->
  input = registry.createReadStream keys: no, gte: 'gtfs/routes', lte: 'gtfs/routes\xff'
  #.........................................................................................................
  input
    .pipe @$add_n4j_system_properties   'route'
    .pipe @$add_id()
    .pipe @$remove_gtfs_fields()
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             1, ( _, sample ) -> whisper 'route', sample
    .pipe P.$on_end                     -> handler null


#===========================================================================================================
# AGENCIES
#-----------------------------------------------------------------------------------------------------------
@read_agency_nodes = ( registry, handler ) ->
  input = registry.createReadStream keys: no, gte: 'gtfs/agency', lte: 'gtfs/agency\xff'
  #.........................................................................................................
  input
    .pipe @$add_n4j_system_properties   'agency'
    .pipe @$add_agency_id()
    .pipe @$remove_gtfs_fields()
    # .pipe REGISTRY.$register            registry
    .pipe P.$show()
    .pipe P.$collect_sample             1, ( _, sample ) -> whisper 'agency', sample
    .pipe P.$on_end                     -> handler null

#-----------------------------------------------------------------------------------------------------------
@$add_agency_id = ( registry ) ->
  return $ ( node, handler ) ->
    debug '©0w3', node
    node[ 'id' ] = node[ 'id' ].replace /^gtfs\/([^-_]+)[-_]+$/, '$1'
    handler null, node


#===========================================================================================================
# COURSES AND TOURS
#-----------------------------------------------------------------------------------------------------------
@read_tour_nodes = ( registry, handler ) ->
  ### OBS implies reading courses and halts. ###
  input = registry.createReadStream keys: no, gte: 'gtfs/trips', lte: 'gtfs/trips\xff'
  #.........................................................................................................
  input
    .pipe @$add_n4j_system_properties   'tour'
    .pipe @$clean_trip_arr_dep()
    .pipe @$tour_from_trip              registry
    .pipe @$add_tour_id()
    # .pipe @$remove_gtfs_fields()
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             1, ( _, sample ) -> whisper 'trip', sample
    .pipe P.$on_end                     -> handler null

#-----------------------------------------------------------------------------------------------------------
@$clean_trip_arr_dep = ->
  ### Replace the first stoptime's arrival and the last stoptime's departure time from the trip. ###
  return $ ( node, handler ) ->
    last_idx = node[ '%gtfs-stoptimes' ].length - 1
    node[ '%gtfs-stoptimes' ][        0 ][ 'arr' ] = null
    node[ '%gtfs-stoptimes' ][ last_idx ][ 'dep' ] = null
    handler null, node

#-----------------------------------------------------------------------------------------------------------
@$tour_from_trip = ( registry ) ->
  return $ ( node, handler ) =>
    reltrip_info    = COURSES.reltrip_info_from_abstrip node
    course          = COURSES.registered_course_from_reltrip_info registry, reltrip_info
    headsign        = @normalize_berlin_station_name reltrip_info[ 'headsign' ]
    gtfs_routes_id  = node[ '%gtfs-routes-id' ]
    route           = registry[ '%gtfs' ][ 'routes' ][ gtfs_routes_id ]
    route_id        = route[ 'id' ]
    #.......................................................................................................
    tour =
      '~isa':       'node'
      '~label':     'tour'
      'course-id':      reltrip_info[ 'course-id'     ]
      'route-id':       route_id
      'headsign':       headsign
      'offset.hhmmss':  reltrip_info[ 'offset.hhmmss' ]
      'offset.s':       reltrip_info[ 'offset.s'      ]
    #.......................................................................................................
    handler null, tour

#-----------------------------------------------------------------------------------------------------------
@$add_tour_id = ->
  idxs = {}
  return $ ( node, handler ) =>
    route_id      = node[ 'route-id' ]
    idx           = idxs[ route_id ] = ( idxs[ route_id ]?= 0 ) + 1
    node[ 'id' ]  = "tour/#{route_id}/#{idx}"
    handler null, node


#===========================================================================================================
#
#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  # debug '©7u9', 'skipping'      # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  # return handler null, registry # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  t0        = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  tasks     = []
  no_source = []
  no_method = []
  ok_types  = []
  #.......................................................................................................
  for node_type in options[ 'data' ][ 'node-types' ]
    if node_type not in [ 'agency', ]  # <<<<<<<<<<
      warn "skipping #{node_type}"  # <<<<<<<<<<
      continue                    #
    #.....................................................................................................
    method_name = "read_#{node_type}_nodes"
    method      = @[ method_name ]
    unless method?
      no_method.push "no method to read nodes of type #{rpr node_type}; skipping"
      continue
    method = method.bind @
    ok_types.push node_type
    #.....................................................................................................
    do ( method_name, method ) =>
      tasks.push ( async_handler ) =>
        help "#{badge}/#{method_name}"
        method registry, ( P... ) =>
          info "#{badge}/#{method_name}"
          async_handler P...
  #.......................................................................................................
  for messages in [ no_source, no_method, ]
    for message in messages
      warn message
  #.......................................................................................................
  info "reading data for #{ok_types.length} type(s)"
  info "  (#{ok_types.join ', '})"
  #.........................................................................................................
  ASYNC.series tasks, ( error ) =>
    throw error if error?
    t1 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    urge 'dt:', ( t1 - t0 ) / 1000 # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    handler null, registry
  #.........................................................................................................
  return null


