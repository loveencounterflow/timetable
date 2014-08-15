

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
# GTFS_READER               = require '../GTFS-READER'
# READER                    = require '../READER'
REGISTRY                  = require '../REGISTRY'
options                   = require '../../options'
#...........................................................................................................
P                         = require 'pipedreams'
$                         = P.$.bind P

# debug new Buffer '\x00\uffff\x00'
# debug new Buffer '\x00𠀀\x00'

# debug rpr ( new Buffer 'abcö' )

#-----------------------------------------------------------------------------------------------------------
new_lte = ( text ) ->
  length  = Buffer.byteLength text
  R       = new Buffer 1 + length
  R.write text
  R[ length ] = 0xff
  return R

# debug ( new_lte 'BVG' ).toString 'utf-8'

"""

# realm / type / ID

  %|gtfs/routes|gtfs/agency/0NV___|11'
  %|gtfs/stop_times|gtfs/stops/9001103|59'
  %|gtfs/stop_times|gtfs/trips/100288|140'
  %|gtfs/trips|gtfs/routes/1001|124813'
  %|gtfs/trips|gtfs/service/000000|105962'

  %  |  gtfs/routes  |  gtfs/agency/BVB---  |  330
       type            ref-id                 id

  %  |  gtfs/routes  |  gtfs/agency/BVB---  |  330
       type            ref-id                 id


  %|gtfs/routes|name:S5|549

"""

#-----------------------------------------------------------------------------------------------------------
@show_routes_of_agency = ( db, agency_id, handler ) ->
  help 'show_routes_of_agency'
  key = "%|gtfs/routes|gtfs/agency/#{agency_id}"
  query =
    'gte':         key
    'lte':         new_lte key
    'keys':         yes
    'values':       no
  help query
  input = ( db.createReadStream query )
    .pipe $ ( record, handler ) ->
      handler null, record
    .pipe P.$show()
    .pipe P.$on_end ->
      return handler null, db

#-----------------------------------------------------------------------------------------------------------
$filter = ( f ) ->
  return $ ( record, handler ) ->
    try
      return handler() unless f record
    catch error
      return handler error
    return handler null, record

#-----------------------------------------------------------------------------------------------------------
@show_subway_routes = ( db, handler ) ->
  help 'show_subway_routes'
  seen_stops  = {}
  ### TAINT must escape name ###
  key = "%|gtfs/routes|name:U"
  key = "gtfs/routes/"
  query =
    'gte':         key
    'lte':         new_lte key
    'keys':         no
    'values':       yes
  help key
  input = ( db.createReadStream query )
    .pipe $filter ( record ) ->
      if /^U/.test name = record[ 'name' ]
        return true
      # whisper name
      return false
    .pipe P.$show()

#-----------------------------------------------------------------------------------------------------------
@show_db_sample = ( db, handler ) ->
  db.createReadStream gt: new_lte '%'
    .pipe P.$sample                     1 / 1e5, seed: 5
    .pipe $ ( { key, value, }, handler ) ->
      return handler null, key if value is true
      return handler null, value
    .pipe P.$show()
    .on 'end', -> handler null, null

#-----------------------------------------------------------------------------------------------------------
@show_stops_of_route = ( db, route_name, handler ) ->
  help 'show_stops_of_route'
  seen_stops  = {}
  ### TAINT must escape name ###
  key = "%|gtfs/routes|name:#{route_name}|"
  query =
    'gte':         key
    'lte':         new_lte key
    'keys':         yes
    'values':       no
  whisper key
  input = ( db.createReadStream query )
    .pipe $ ( record, handler ) ->
      [ ..., route_id ] = record.split '|'
      key = "%|gtfs/trips|gtfs/routes/#{route_id}|"
      query =
        'gte':         key
        'lte':         new_lte key
        'keys':         yes
        'values':       no
      whisper key
      input = ( db.createReadStream query )
        # .pipe P.$show()
        .pipe $ ( record ) ->
          [ ..., trip_id ] = record.split '|'
          # whisper trip_id
          key = "%|gtfs/stop_times|gtfs/trips/#{trip_id}|"
          query =
            'gte':         key
            'lte':         new_lte key
            'keys':         yes
            'values':       no
          # whisper key
          input = ( db.createReadStream query )
            # .pipe P.$show()
            .pipe $ ( record ) ->
              [ _, type, _, id ] = record.split '|'
              stoptime_key = "#{type}/#{id}"
              # whisper stoptime_key
              db.get stoptime_key, ( error, record ) ->
                return handler error if error?
                stop_key = "gtfs/stops/#{record[ 'gtfs-stops-id' ]}"
                db.get stop_key, ( error, record ) ->
                  return handler error if error?
                  name = record[ 'name' ]
                  return if name of seen_stops
                  seen_stops[ name ] = 1
                  help name

#-----------------------------------------------------------------------------------------------------------
$_XXX_show = ( label ) ->
  return $ ( record, handler ) ->
    log ( TRM.grey label ), ( TRM.lime record )
    handler null, record

#-----------------------------------------------------------------------------------------------------------
$read_trips_from_route = ( db ) ->
  help 'read_trips_from_route'
  return P.through ( record_1 ) ->
    [ ..., route_id ] = record_1.split '|'
    key = "%|gtfs/trips|gtfs/routes/#{route_id}|"
    query =
      'gte':         key
      'lte':         new_lte key
      'keys':         yes
      'values':       no
    whisper key
    emit = @emit.bind @
    # emit 'data', 'starting nested search'
    count = 0
    db.createReadStream query
      # .pipe $_XXX_show '2'
      .pipe P.through ( record_2 ) ->
        # whisper record_2
        emit 'data', record_2
        count += 1
      ,
        -> whisper count
        # @emit 'data', record_2
        # handler_1 null, record_2
    # for idx in [ 0 .. 5 ]
    #       do ( idx ) ->
    #         setTimeout ( -> emit 'data', idx ), 0

# #-----------------------------------------------------------------------------------------------------------
# @_pass_on = ( handler ) ->
#   return ( record ) ->
#     handler null, record
#     @emit 'data', record

# #-----------------------------------------------------------------------------------------------------------
# @_signal_end = ( handler ) ->
#   return ( record ) ->
#     handler null, null
#     @emit 'end'

# #-----------------------------------------------------------------------------------------------------------
# @read_routes_id_from_name = ( db, route_name, handler ) ->
#   help 'read_routes_id_from_name'
#   seen_stops  = {}
#   ### TAINT must escape name ###
#   key = "%|gtfs/routes|name:#{route_name}"
#   query =
#     'gte':          key
#     'lte':          new_lte key
#     'keys':         yes
#     'values':       no
#   whisper key
#   #.........................................................................................................
#   pass_on = ( record ) ->
#     [ _, _, facet, id, ]  = record.split '|'
#     [ _, name, ]          = facet.split ':'
#     record_id   = "gtfs/routes/#{id}"
#     Z           = 'id': record_id
#     Z[ 'name' ] = name
#     handler null, Z
#     @emit 'data', Z
#   #.........................................................................................................
#   ( db.createReadStream query ).pipe P.through pass_on, ( @_signal_end handler )


#-----------------------------------------------------------------------------------------------------------
@$read_trips_from_route = ( db ) ->
  help 'read_trips_from_route'
  return P.through ( record_1 ) ->
    [ ..., route_id ] = record_1.split '|'
    key = "%|gtfs/trips|gtfs/routes/#{route_id}|"
    query =
      'gte':         key
      'lte':         new_lte key
      'keys':         yes
      'values':       no
    whisper key
    emit = @emit.bind @
    # emit 'data', 'starting nested search'
    count = 0
    db.createReadStream query
      # .pipe $_XXX_show '2'
      .pipe P.through ( record_2 ) ->
        # whisper record_2
        emit 'data', record_2
        count += 1
      ,
        -> whisper count

#-----------------------------------------------------------------------------------------------------------
@read_routes_id_from_name = ( db, route_name, handler ) ->
  help 'read_routes_id_from_name'
  ### TAINT must escape name ###
  key = "%|gtfs/routes|name:#{route_name}"
  query =
    'gte':          key
    'lte':          new_lte key
    'keys':         yes
    'values':       no
  whisper key
  #.........................................................................................................
  db.createReadStream query
    #.......................................................................................................
    .on 'data', ( record ) ->
      [ _, _, facet, id, ]  = record.split '|'
      [ _, name, ]          = facet.split ':'
      record_id   = "gtfs/routes/#{id}"
      Z           = 'id': record_id
      Z[ 'name' ] = name
      handler null, Z
    #.......................................................................................................
    .on 'end', -> handler null, null

#-----------------------------------------------------------------------------------------------------------
@read_trips_id_from_route_id = ( db, route_id, handler ) ->
  help 'read_trips_id_from_route_id'
  ### TAINT must escape name ###
  key = "%|gtfs/trips|#{route_id}|"
  query =
    'gte':          key
    'lte':          new_lte key
    'keys':         yes
    'values':       no
  # whisper '5', key
  #.........................................................................................................
  db.createReadStream query
    #.......................................................................................................
    .on 'data', ( record ) ->
      # whisper record
      [ _, gtfs_type, _, id, ]  = record.split '|'
      record_id                 = "#{gtfs_type}/#{id}"
      Z                         = 'id': record_id
      handler null, Z
    #.......................................................................................................
    .on 'end', -> handler null, null

#-----------------------------------------------------------------------------------------------------------
@read_stoptimes_id_from_trips_id = ( db, trip_id, handler ) ->
  help 'read_stoptimes_id_from_trips_id'
  key = "%|gtfs/stop_times|#{trip_id}|"
  query =
    'gte':          key
    'lte':          new_lte key
    'keys':         yes
    'values':       no
  # whisper '5', key
  #.........................................................................................................
  db.createReadStream query
    #.......................................................................................................
    .on 'data', ( record ) ->
      # whisper '7', record
      [ _, gtfs_type, _, id, ]  = record.split '|'
      record_id                 = "#{gtfs_type}/#{id}"
      Z                         = 'id': record_id
      handler null, Z
    #.......................................................................................................
    .on 'end', -> handler null, null

#-----------------------------------------------------------------------------------------------------------
@f = ( db, route_name, handler ) ->
  @read_routes_id_from_name db, route_name, ( error, record ) =>
    return handler error if error?
    return if record is null
    { id: route_id, name } = record
    whisper '1', route_id, name
    @read_trips_id_from_route_id db, route_id, ( error, record ) =>
      return handler error if error?
      return if record is null
      trip_id = record[ 'id' ]
      @read_stoptimes_id_from_trips_id db, trip_id, ( error, record ) =>
        return handler error if error?
        return if record is null
        stoptimes_id = record[ 'id' ]
        db.get stoptimes_id, ( error, record ) =>
          return handler error if error?
          debug record
        # return handler null, null if record is null


# %|gtfs/agency|name:A. Reich GmbH Busbetrieb|REI---
# %|gtfs/routes|gtfs/agency/0NV___|11
# %|gtfs/routes|name:1%2F411|1392
# %|gtfs/routes|name:|811
# %|gtfs/stop_times|gtfs/stops/8012117|181
# %|gtfs/stop_times|gtfs/trips/100497|141
# %|gtfs/stops|name:Aalemannufer (Berlin)|9027205
# %|gtfs/trips|gtfs/routes/1001|124813
# %|gtfs/trips|gtfs/service/000000|105962

# %|gtfs/stop_times>gtfs/stops/8012117|181
# %|gtfs/stops<gtfs/stop_times/181|8012117

#-----------------------------------------------------------------------------------------------------------
@show_unique_indexes = ( db, handler ) ->
  help 'show_unique_indexes'
  seen  = {}
  count = 0
  query =
    'gte':         '%|gtfs'
    'lte':         new_lte '%|gtfs'
    # 'gte':          'gtfs/stops'
    # 'lte':          'gtfs/stops/\xff'
    'keys':         yes
    'values':       no
    # 'keyEncoding': 'binary'
  # input = ( db.createReadStream options[ 'levelup' ][ 'new' ] )
  help query
  input = ( db.createReadStream query )
    .pipe $ ( record, handler ) ->
      count += 1
      handler null, record
    .pipe $ ( record, handler ) ->
      [ _, gtfs_type, gtfs_ref_id, gtfs_id ] = record.split '|'
      gtfs_ref_type = gtfs_ref_id.replace /[\/:][^\/:]+$/, ''
      key = gtfs_type + '|' + gtfs_ref_type
      return handler() if key of seen
      seen[ key ] = 1
      handler null, record
    .pipe P.$show()
    .pipe P.$on_end ->
      help count, "records in DB"
      return handler null, db


#-----------------------------------------------------------------------------------------------------------
@main = ( handler ) ->
  registry = REGISTRY.new_registry()
  # @show_unique_indexes registry, handler
  # @show_routes_of_agency registry, 'BVB---', handler
  # @show_subway_routes registry, handler
  # @show_stops_of_route registry, 'U4', handler
  # @show_stops_of_route_2 registry, 'U4', handler
  @f registry, 'U1', ( error, record ) -> debug record
  # @show_db_sample registry, handler
  return null


############################################################################################################
unless module.parent?
  @main ( error, registry ) =>
    throw error if error?
    help 'ok'
