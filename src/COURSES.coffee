

### Given a `trip`, return a `reltrip-info` object', which is a `reltrip` object with a `course` object.
Example for a reltrip info object:

    { '~isa': 'reltrip',
      'course-id': 'course/537f81c06',
      'offset.hhmmss': '14:04:00',
      'offset.s': 50640,
      'course': [
        { stop_id: '9130002', arrdep_rel: [ 0, 0     ] },
        { stop_id: '9130011', arrdep_rel: [ 60, 60   ] },
        { stop_id: '9110001', arrdep_rel: [ 180, 180 ] },
        { stop_id: '9110006', arrdep_rel: [ 300, 300 ] }
        ] }

A `reltrip` object is obtained by taking the course out, leaving just the course ID, and adding any
missing attributes from the trip object.

A course details both the stop identities and arrival and departure times in seconds relative to that
trip's first departure; the course ID is obtained by calculating a cryptographic hash digest from the stop
IDs and the relative times. This procedure means that all trips that visit the same stops in the same
order and the same relative travelling times have identical course IDs, which helps both in identifying
regularities in the timetable and to reduce the amount of data needed. Actual times may be reconstructed
by adding the offset given to each arrival and departure time for a given trip.

The term 'course' is inspired by the German word *Kurswagen* meaning *through coach*, presumably so named
because a through coach follows its own course (sequence of stops). ###



############################################################################################################
# njs_util                  = require 'util'
# njs_path                  = require 'path'
# njs_fs                    = require 'fs'
njs_crypto                  = require 'crypto'
#...........................................................................................................
# BAP                       = require 'coffeenode-bitsnpieces'
# TYPES                     = require 'coffeenode-types'
TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'TIMETABLE/course-from-trip' # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
log                       = TRM.get_logger 'plain',     badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
info                      = TRM.get_logger 'info',      badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
whisper                   = TRM.get_logger 'whisper',   badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
alert                     = TRM.get_logger 'alert',     badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
debug                     = TRM.get_logger 'debug',     badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
warn                      = TRM.get_logger 'warn',      badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
help                      = TRM.get_logger 'help',      badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
urge                      = TRM.get_logger 'urge',      badge # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
echo                      = TRM.echo.bind TRM # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
rainbow                   = TRM.rainbow.bind TRM # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#...........................................................................................................
REGISTRY                  = require './REGISTRY'


#-----------------------------------------------------------------------------------------------------------
@reltrip_info_from_abstrip = ( trip ) ->
  stoptimes       = trip[ '%gtfs-stoptimes' ]
  return null if stoptimes.length is 0
  course_id       = []
  halts           = []
  offset_hhmmss   = stoptimes[ 0 ][ 'dep' ] ? stoptimes[ 0 ][ 'arr' ]
  offset_s        = @_seconds_from_time_text offset_hhmmss
  # gtfs_routes_id  = trip[ '%gtfs-routes-id' ]
  # gtfs_service_id = trip[ '%gtfs-service-id' ]
  #.........................................................................................................
  R             =
    '~isa':             'node'
    '~label':           'reltrip-info'
    'id':               null
    'course-id':        null
    'headsign':         trip[ 'headsign' ]
    # '%gtfs-routes-id':  gtfs_routes_id
    # '%gtfs-service-id': gtfs_service_id
    'offset.hhmmss':    offset_hhmmss
    'offset.s':         offset_s
    'halts':            halts
  #.........................................................................................................
  for stoptime, halt_idx in stoptimes
    gtfs_stop_id    = stoptime[ '%gtfs-stop-id' ]
    arr_hhmmss      = stoptime[ 'arr' ]
    dep_hhmmss      = stoptime[ 'dep' ]
    arr_rel         = if arr_hhmmss? then ( @_seconds_from_time_text arr_hhmmss ) - offset_s else null
    dep_rel         = if dep_hhmmss? then ( @_seconds_from_time_text dep_hhmmss ) - offset_s else null
    ### TAINT contains duplications ###
    halt =
      '~isa':             'node'
      '~label':           'halt'
      'idx':              halt_idx
      'id':               null
      '%gtfs-stop-id':    gtfs_stop_id
      'course-id':        null
      # '%gtfs-routes-id':  gtfs_routes_id
      'arr-rel.s':        arr_rel
      'dep-rel.s':        dep_rel
      'display':          @_format_relative_seconds dep_rel ? arr_rel
    halts.push halt
    course_id.push "#{gtfs_stop_id},#{arr_rel},#{dep_rel}"
  #.........................................................................................................
  course_id           = course_id.join ';'
  digest              = @_digest_from_text course_id
  R[ 'course-id' ]    = course_id = 'course/' + digest
  for halt, halt_idx in halts
    halt_id             = "halt/#{course_id.replace '/', ':'}/#{halt_idx}"
    halt[ 'id' ]        = halt_id
    halt[ 'course-id' ] = course_id
  #.........................................................................................................
  # debug '©6z3', R; process.exit()
  return R

#-----------------------------------------------------------------------------------------------------------
@registered_course_from_reltrip_info = ( registry, reltrip_info ) ->
  course_id = reltrip_info[ 'course-id' ]
  R         = registry[ 'course' ][ course_id ]
  return R if R?
  R =
    '~isa':           'node'
    '~label':         'course'
    'id':             course_id
    'headsign':       reltrip_info[ 'headsign' ]
  REGISTRY.register registry, R
  for halt in reltrip_info[ 'halts' ]
    gtfs_stop_id          = halt[ '%gtfs-stop-id' ]
    station               = registry[ '%gtfs' ][ 'stops' ][ gtfs_stop_id ]
    station_id            = station[ 'id' ]
    halt[ 'station-id' ]  = station_id
    delete halt[ '%gtfs-stop-id' ]
    REGISTRY.register registry, halt
  return R


#===========================================================================================================
# HELPERS
#-----------------------------------------------------------------------------------------------------------
@_digest_from_text = ( text ) ->
  ### TAINT arbitrarily shortened ID ###
  hash = ( njs_crypto.createHash 'sha1' ).update text, 'utf-8'
  return ( hash.digest 'hex' )[ .. 8 ]

#-----------------------------------------------------------------------------------------------------------
@_seconds_from_time_text = ( time_txt ) ->
  ### OBS We do not use an established module for converting a time of the day to seconds since the GTFS
  data use the (very sensible) convention to represent times of a trip that continues after midnight by
  hours 24 and greater (so when you leave a place at 23:10 one day and arrive at 01:44 the following day,
  the GTFS data will represent 01:44 as 25:44). ###
  match = time_txt.match /([0-9]{2}):([0-9]{2}):([0-9]{2})/
  throw new Error "invalid time text #{rpr time_txt}" unless match?
  [ ignore
    hours_txt
    minutes_txt
    seconds_txt ] = match
  hours           = parseInt   hours_txt, 10
  minutes         = parseInt minutes_txt, 10
  seconds         = parseInt seconds_txt, 10
  minutes         = minutes +   hours * 60
  seconds         = seconds + minutes * 60
  return seconds

#-----------------------------------------------------------------------------------------------------------
@_format_relative_seconds = ( seconds ) ->
  minutes = seconds // 60
  hours   = minutes // 60
  minutes = minutes %% 60
  seconds = seconds %% 60
  # hours   = TEXT.flush_right   hours, 2, '0'
  minutes = TEXT.flush_right minutes, 2, '0'
  seconds = TEXT.flush_right seconds, 2, '0'
  return "+#{hours}:#{minutes}:#{seconds}"


