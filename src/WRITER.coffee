

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


#-----------------------------------------------------------------------------------------------------------
@_distance_from_latlongs = ( latlong_a, latlong_b ) ->
  ### http://hashbang.co.nz/blog/2013/2/25/d3_js_geo_fun ###
  # calcDist: (p1,p2) ->
  #   #Haversine formula
  #   dLatRad = Math.abs(p1[1] - p2[1]) * Math.PI/180;
  #   dLonRad = Math.abs(p1[0] - p2[0]) * Math.PI/180;
  #   # Calculate origin in Radians
  #   lat1Rad = p1[1] * Math.PI/180;
  #   lon1Rad = p1[0] * Math.PI/180;
  #   # Calculate new point in Radians
  #   lat2Rad = p2[1] * Math.PI/180;
  #   lon2Rad = p2[0] * Math.PI/180;

  #   # Earth's Radius
  #   eR = 6371;
  #   d1 = Math.sin(dLatRad/2) * Math.sin(dLatRad/2) +
  #      Math.sin(dLonRad/2) * Math.sin(dLonRad/2) * Math.cos(lat1Rad) * Math.cos(lat2Rad);
  #   d2 = 2 * Math.atan2(Math.sqrt(d1), Math.sqrt(1-d1));
  #   return(eR * d2);

  ### http://www.geodatasource.com/developers/javascript ###
  [ lat1, lon1, ] = latlong_a
  [ lat2, lon2, ] = latlong_b
  # [ lon1, lat1, ] = latlong_a
  # [ lon2, lat2, ] = latlong_b
  radlat1 = Math.PI * lat1 / 180
  radlat2 = Math.PI * lat2 / 180
  radlon1 = Math.PI * lon1 / 180
  radlon2 = Math.PI * lon2 / 180
  theta   = ( lon1 - lon2 ) * Math.PI / 180
  R       = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(theta)
  R       = Math.acos R
  R       = R * 180 / Math.PI
  R       = R * 60 * 1.1515
  R       = R * 1.609344
  R       = R * 1000
  return R

#-----------------------------------------------------------------------------------------------------------
@create_indexes = ( handler ) ->
  #.........................................................................................................
  tasks       = []
  constraints = [
    "CREATE CONSTRAINT ON (n:trip)     ASSERT n.id IS UNIQUE;"
    "CREATE CONSTRAINT ON (n:stop)     ASSERT n.id IS UNIQUE;"
    "CREATE CONSTRAINT ON (n:route)    ASSERT n.id IS UNIQUE;"
    "CREATE CONSTRAINT ON (n:stoptime) ASSERT n.id IS UNIQUE;" ]
  #.........................................................................................................
  identifiers = [
    # ':trip(`id`)'
    # ':stop(`id`)'
    # ':route(`id`)'
    # ':stoptime(`id`)'
    ':trip(`route-id`)'
    ':stop(`name`)'
    ':stoptime(`trip-id`)'
    ':stoptime(`stop-id`)'
    ':route(`name`)' ]
  #.........................................................................................................
  for constraint in constraints
    do ( constraint ) =>
      query = query: constraint
      tasks.push ( async_handler ) =>
        N4J._request query, ( error ) =>
          return async_handler error if error?
          whisper "created constraint and index with #{constraint}"
          async_handler null
  #.........................................................................................................
  for identifier in identifiers
    do ( identifier ) =>
      query = query: """CREATE INDEX ON #{identifier};"""
      tasks.push ( async_handler ) =>
        N4J._request query, ( error ) =>
          return async_handler error if error?
          whisper "created index on #{identifier}"
          async_handler null
  #.........................................................................................................
  on_finish = ( error ) =>
    return handler error if error?
    handler null
  #.........................................................................................................
  ASYNC.series tasks, on_finish

#-----------------------------------------------------------------------------------------------------------
@update_distances = ( handler ) ->
  query   = query: """MATCH (stop1:stop)-[r:distance]->(stop2:stop) RETURN DISTINCT stop1,stop2"""
  #---------------------------------------------------------------------------------------------------------
  N4J._request query, ( error, rows ) =>
    return handler error if error?
    tasks = []
    for [ stop1, stop2 ] in rows
      id1         = stop1[ 'id' ]
      id2         = stop2[ 'id' ]
      distance_m  = Math.floor 0.5 + @_distance_from_latlongs stop1[ 'latlong' ], stop2[ 'latlong' ]
      query       = query: """
        MATCH (stop1:stop {id: #{N4J._escape id1}})-[r:distance]->(stop2:stop {id: #{N4J._escape id2}})
        SET r.value = #{distance_m}"""
      do ( query ) =>
        tasks.push ( async_handler ) =>
          N4J._request query, ( error ) =>
            return async_handler error if error?
            # whisper query[ 'query' ]
            async_handler null
    #-------------------------------------------------------------------------------------------------------
    on_finish = ( error ) =>
      return handler error if error?
      handler null
    #-------------------------------------------------------------------------------------------------------
    ASYNC.series tasks, on_finish
  #---------------------------------------------------------------------------------------------------------
  return null

#-----------------------------------------------------------------------------------------------------------
@create_edges = ( handler ) ->
  tasks   = []
  ### TAINT rewrite for halts, courses, reltrips ###
  # """
  #   MATCH (stop1:stop)--(a:stoptime)-[:linked]-(b:stoptime)--(stop2:stop)
  #   CREATE (stop1)-[:distance {`~label`: 'distance', value: 0}]->(stop2)
  # """
  sources = [
    """
      MATCH (a:halt)
      MATCH (b:halt {`course-id`: a.`course-id`, `idx`: a.idx + 1})
      CREATE (a)-[:linked {`~label`: 'linked'}]->(b);"""
    ]
  #.....................................................................................................
  for source in sources
    do ( source ) =>
      query = query: source
      tasks.push ( async_handler ) =>
        N4J._request query, ( error ) =>
          return async_handler error if error?
          whisper "created edges with\n#{source}"
          async_handler null
  #.....................................................................................................
  on_finish = ( error ) =>
    return handler error if error?
    handler null
  #.......................................................................................................
  ASYNC.series tasks, on_finish
  return null


#===========================================================================================================
#
#-----------------------------------------------------------------------------------------------------------
@read_station_nodes = ( registry, handler ) ->
  # ratio = if DEV then 1 / 100 else 1
  input = P.$read_values registry[ '%gtfs' ][ 'stops' ]
  #.........................................................................................................
  input
    # .pipe P.$sample                     ratio, headers: true, seed: 5 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    .pipe @$normalize_station_name      'name'
    .pipe @$add_n4j_system_properties   'station'
    .pipe @$add_id()
    .pipe @$remove_gtfs_fields()
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'station', sample
    .on 'end', =>
      return handler null



#===========================================================================================================
#
#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  t0        = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  tasks     = []
  #.......................................................................................................
  for node_type in options[ 'data' ][ 'node-types' ]
    collection = registry[ node_type ]
    # for
  # #.......................................................................................................
  # for messages in [ no_source, no_method, ]
  #   for message in messages
  #     warn message
  # #.......................................................................................................
  # info "reading data for #{ok_types.length} type(s)"
  # info "  (#{ok_types.join ', '})"
  #.........................................................................................................
  ASYNC.series tasks, ( error ) =>
    throw error if error?
    t1 = 1 * new Date() # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    urge 'dt:', ( t1 - t0 ) / 1000 # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    handler null, registry
  #.........................................................................................................
  return null


