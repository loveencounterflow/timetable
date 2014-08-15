


"""




g/sttm^828812
g/trip^32565
g/route^811


%|g/route|name:foobar|^811


%|gtfs/stoptime|gtfs/trip^123|^556|gtfs/service^33421

  %|gtfs/stoptime|gtfs/trip^123|^556
+ %|gtfs/trip|gtfs/route^8877|^123
------------------------------------------------------
= %|gtfs/stoptime|gtfs/route^8877


= %|gtfs/stoptime|gtfs/trip^123|^556|gtfs/route^8877


############################################################################################################

gtfs:
  stoptime:
    id:           gtfs/stoptime/^876
    stop-id:      gtfs/stop/^123
    trip-id:      gtfs/trip/^456
    ...

  stop:
    id:           gtfs/stop/^123
    name:         Bayerischer+Platz
    ...

  trip:
    id:           gtfs/trip/^456
    route-id:     gtfs/route/^777
    service-id:   gtfs/service/^888


gtfs/stoptime/^876
gtfs/stoptime/^876/stop-id:gtfs/stop/^123
gtfs/stoptime/^876/trip-id:gtfs/trip/^456
gtfs/stop/^123
gtfs/stop/^123/name:Bayerischer+Platz
gtfs/trip/^456


############################################################################################################

gtfs:
  stoptime:
    ^:                gtfs/stoptime/^876
    gtfs/stop/^:      gtfs/stop/^123
    gtfs/trip/^:      gtfs/trip/^456
    ...
    arr:              15:38
    dep:              15:38


  stop:
    ^:                gtfs/stop/^123
    name:             Bayerischer+Platz
    ...

  trip:
    ^:                gtfs/trip/^456
    gtfs/route/^:     gtfs/route/^777
    gtfs/service/^:   gtfs/service/^888

  route:
    ^:                gtfs/route/^777
    name:             U4

# REALM/TYPE/^IDN
REALM/TYPE/^IDN|NAME:VALUE
REALM₀/TYPE₀/^IDN₀|>REALM₁/TYPE₁/^IDN₁

gtfs/route/^777
gtfs/route/^777|name:U4
gtfs/stop/^123
gtfs/stop/^123|=name:Bayerischer+Platz
gtfs/stoptime/^876
gtfs/stoptime/^876|=arr:15%2538
gtfs/stoptime/^876|=dep:15%2538
gtfs/stoptime/^876|>gtfs/stop/^123
gtfs/stoptime/^876|>gtfs/trip/^456
gtfs/trip/^456
gtfs/trip/^456|>gtfs/route/^777
gtfs/trip/^456|>gtfs/service/^888


  gtfs/stoptime/^876|>gtfs/trip/^456
+                     gtfs/trip/^456|>gtfs/route/^777
----------------------------------------------------------------
= gtfs/stoptime/^876|>>gtfs/route/^777


  gtfs/stoptime/^876|>gtfs/trip/^456
                      gtfs/trip/^456|>gtfs/service/^888
----------------------------------------------------------------
= gtfs/stoptime/^876|>>gtfs/service/^888

============================================================================================================

%|REALM/TYPE|=NAME:VALUE|^IDN
%|REALM₀/TYPE₀|>N>=REALM₁/TYPE₁/^IDN₁|^IDN₀

%|gtfs/route|=name:U4|^777
%|gtfs/stoptime|=arr:15%2538|^876
%|gtfs/stoptime|=dep:15%2538|^876
%|gtfs/stoptime|>1>gtfs/stop/^123|^876
%|gtfs/stoptime|>1>gtfs/trip/^456|^876
%|gtfs/stoptime|>2>gtfs/route/^777|^876
%|gtfs/stoptime|>2>gtfs/service/^888|^876
%|gtfs/stop|=name:Bayerischer+Platz|^123
%|gtfs/trip|>gtfs/route/^777|^456
%|gtfs/trip|>gtfs/service/^888|^456

"""


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
@$set_id_from_gtfs_id = ->
  return $ ( record, handler ) =>
    gtfs_id         = record[ 'gtfs-id'  ]
    gtfs_type       = record[ 'gtfs-type'    ]
    return handler new Error "unable to register record without GTFS ID: #{rpr record}"   unless gtfs_id?
    return handler new Error "unable to register record without GTFS type: #{rpr record}" unless gtfs_type?
    record[ 'id' ]  = "gtfs/#{gtfs_type}/#{gtfs_id}"
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$convert_latlon = ->
  return $ ( record, handler ) =>
    record[ 'lat' ] = parseFloat record[ 'lat' ]
    record[ 'lon' ] = parseFloat record[ 'lon' ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$_XXXX_add_id_indexes = ->
  on_data = ( record ) ->
    realm   = 'gtfs'
    type    = record[ 'gtfs-type' ]
    id      = record[ 'gtfs-id' ]
    #.......................................................................................................
    for name, ref_id of record
      match = name.match /^gtfs-(.+?)-id$/
      continue unless match?
      [ _, ref_type ] = match
      ref_realm     = 'gtfs'
      index_record  = 'id': "%|#{realm}/#{type}|#{ref_realm}/#{ref_type}/#{ref_id}|#{id}"
      @emit 'data', index_record
      # index_record  = 'id': "%|#{ref_realm}/#{ref_type}/#{ref_id}<#{realm}/#{type}/#{id}"
      # @emit 'data', index_record
    #.......................................................................................................
    @emit 'data', record
  #.........................................................................................................
  return P.through on_data, null

#-----------------------------------------------------------------------------------------------------------
@$index_on = ( names... ) ->
  escape = @_escape_for_index
  on_data = ( record ) ->
    realm   = 'gtfs'
    type    = record[ 'gtfs-type' ]
    id      = record[ 'gtfs-id' ]
    #.......................................................................................................
    for name in names
      value   = record[ name ]
      continue unless value?
      #.....................................................................................................
      unless ( value_type = TYPES.type_of value ) is 'text'
        throw new Error "building index from type #{rpr value_type} not currently supported"
      #.....................................................................................................
      value         = escape value
      index_record  = 'id': "%|#{realm}/#{type}|#{name}:#{value}|#{id}"
      @emit 'data', index_record
    #.......................................................................................................
    @emit 'data', record
  #.........................................................................................................
  return P.through on_data, null

#-----------------------------------------------------------------------------------------------------------
@_escape_for_index = ( text ) ->
  R = text
  R = R.replace /%/g,   '%25'
  R = R.replace /\|/g,  '%7C'
  R = R.replace /:/g,   '%3A'
  R = R.replace /\//g,  '%2F'
  return R


############################################################################################################
# SPECIFIC METHODS
#===========================================================================================================
# SPECIFIC METHODS: AGENCY
#-----------------------------------------------------------------------------------------------------------
@read_agency = ( registry, route, handler ) ->
  input       = P.create_readstream route, 'agency'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    # .pipe @$_XXX_save() # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # .pipe P.$sample                     1 / 10, headers: true, seed: 5
    .pipe P.$parse_csv()
    .pipe @$clean_agency_record()
    .pipe P.$delete_prefix              'agency_'
    .pipe P.$set                        'gtfs-type', 'agency'
    .pipe P.$rename                     'id', 'gtfs-id'
    .pipe @$set_id_from_gtfs_id()
    .pipe @$clean_agency_record()
    .pipe P.$dasherize_field_names()
    .pipe @$index_on                    'name' # , 'url'
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             4, ( _, sample ) -> whisper 'agency', sample
    .pipe P.$on_end                     -> handler null
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
  ratio       = if DEV then 1 / 10 else 1
  input       = P.create_readstream route, 'routes'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$sample                     ratio, headers: true, seed: 5
    .pipe P.$parse_csv()
    .pipe @$clean_routes_record()
    .pipe P.$dasherize_field_names()
    .pipe P.$set                        'gtfs-type',       'routes'
    .pipe P.$rename                     'route-id',         'gtfs-id'
    .pipe P.$rename                     'agency-id',        'gtfs-agency-id'
    .pipe P.$rename                     'route-short-name', 'name'
    .pipe @$set_id_from_gtfs_id()
    .pipe @$_XXXX_add_id_indexes()
    .pipe @$index_on                    'name'
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             8, ( _, sample ) -> whisper 'route', sample
    .pipe P.$on_end                     -> handler null
  #.........................................................................................................
  return null

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
  input       = P.create_readstream route, 'calendar_dates'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe P.$sample                     1 / 100, headers: true, seed: 5
    .pipe @$clean_calendar_date_record()
    .pipe P.$set                        'gtfs-type', 'calendar_dates'
    .pipe P.$rename                     'service_id', 'gtfs-id'
    .pipe @$set_id_from_gtfs_id()
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             4, ( _, sample ) -> whisper 'service', sample
    .pipe P.$on_end                     -> handler null
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@$clean_calendar_date_record = ->
  return $ ( record, handler ) =>
    delete record[ 'exception_type' ]
    handler null, record



#===========================================================================================================
# SPECIFIC METHODS: TRIPS
#-----------------------------------------------------------------------------------------------------------
@read_trips = ( registry, route, handler ) ->
  input       = P.create_readstream route, 'trips'
  ratio       = if DEV then 1 / 10 else 1
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe P.$sample                     ratio, headers: true, seed: 5
    .pipe @$clean_trip_record()
    .pipe P.$delete_prefix              'trip_'
    .pipe P.$dasherize_field_names()
    .pipe P.$set                        'gtfs-type',       'trips'
    # .pipe P.$set                        'gtfs-stoptimes',  null
    .pipe P.$rename                     'id',               'gtfs-id'
    .pipe P.$rename                     'route-id',         'gtfs-routes-id'
    .pipe P.$rename                     'service-id',       'gtfs-service-id'
    .pipe @$set_id_from_gtfs_id()
    .pipe @$_XXXX_add_id_indexes()
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             4, ( _, sample ) -> whisper 'trip', sample
    .pipe P.$on_end                     -> handler null
  #.........................................................................................................
  return null

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
  input = P.create_readstream route, 'stop_times'
  ratio = if DEV then 1 / 1000 else 1
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$sample                     ratio, headers: true
    .pipe P.$parse_csv()
    .pipe @$clean_stop_times_record()
    .pipe P.$set                        'gtfs-type', 'stop_times'
    .pipe P.$dasherize_field_names()
    .pipe P.$rename                     'trip-id',        'gtfs-trips-id'
    .pipe P.$rename                     'stop-id',        'gtfs-stops-id'
    .pipe P.$rename                     'arrival-time',   'arr'
    .pipe P.$rename                     'departure-time', 'dep'
    .pipe @$add_stoptimes_gtfs_id()
    .pipe @$set_id_from_gtfs_id()
    # .pipe @$add_stoptime_to_trip        registry # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    .pipe @$_XXXX_add_id_indexes()
    # .pipe @$_XXXX_add_stop_times_index()
    .pipe REGISTRY.$register            registry
    # .pipe @$register_stop_id            registry
    # .pipe @$_XXX_save() # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    .pipe P.$collect_sample             5, ( _, sample ) -> whisper 'stop_time', sample
    .pipe P.$on_end                     -> handler null
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
@$add_stoptimes_gtfs_id = ->
  idx = 0
  return $ ( record, handler ) =>
    record[ 'gtfs-id' ] = "#{idx}"
    idx += 1
    handler null, record


#===========================================================================================================
# SPECIFIC METHODS: STOPS
#-----------------------------------------------------------------------------------------------------------
@read_stops = ( registry, route, handler ) ->
  input = P.create_readstream route, 'stops'
  #.........................................................................................................
  input.pipe P.$split()
    .pipe P.$skip_empty()
    .pipe P.$parse_csv()
    .pipe @$clean_stops_record()
    .pipe P.$delete_prefix              'stop_'
    .pipe P.$set                        'gtfs-type', 'stops'
    # .pipe P.$copy                       'name', 'gtfs-name'
    .pipe P.$rename                     'id', 'gtfs-id'
    .pipe @$set_id_from_gtfs_id()
    .pipe @$convert_latlon()
    .pipe @$index_on                    'name'
    .pipe REGISTRY.$register            registry
    .pipe P.$collect_sample             4, ( _, sample ) -> whisper 'stop', sample
    .pipe P.$on_end                     -> handler null
  #.........................................................................................................
  return null

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
      # if DEV
      #   if gtfs_type in [ 'agency', 'stops', ]  # <<<<<<<<<<
      #     warn "skipping #{gtfs_type}"  # <<<<<<<<<<
      #     continue                    # <<<<<<<<<<
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
      do ( method_name, method, route ) =>
        tasks.push ( async_handler ) =>
          help "#{badge}/#{method_name}"
          method registry, route, ( P... ) =>
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




############################################################################################################
# HELPERS
#===========================================================================================================

############################################################################################################
unless module.parent?
  registry = REGISTRY.new_registry()
  @main registry, ( error, registry ) ->
    throw error if error?
    info registry




