


############################################################################################################
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'TIMETABLE/REGISTRY'
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



#-----------------------------------------------------------------------------------------------------------
@new_registry = ->
  R =
    '~isa':           'TIMETABLE/registry'
    '%gtfs':          {}
    '%state':         {}
  R[ '%gtfs' ][ gtfs_type ] = {} for gtfs_type in options[ 'data' ][ 'gtfs-types' ]
  R[                 type ] = {} for      type in options[ 'data' ][ 'node-types' ]
  return R

#-----------------------------------------------------------------------------------------------------------
@register_gtfs = ( registry, record ) ->
  gtfs_id = record[ '%gtfs-id' ]
  unless gtfs_id?
    throw new Error """
      unable to register record without GTFS ID:
      #{rpr record}"""
  #.......................................................................................................
  gtfs_type = record[ '%gtfs-type' ]
  unless gtfs_type?
    throw new Error """
      unable to register record without GTFS type:
      #{rpr record}"""
  #.......................................................................................................
  sub_registry = registry[ '%gtfs' ]?[ gtfs_type ]
  throw new Error "unable to locate registry for GTFS type #{rpr gtfs_type}" unless sub_registry?
  if ( dupe = sub_registry[ gtfs_id ] )? # and dupe isnt record
    throw new Error """already registered:
      #{rpr dupe}
      #{rpr record}"""
  #.......................................................................................................
  sub_registry[ gtfs_id ] = record
  return null

#-----------------------------------------------------------------------------------------------------------
@register = ( registry, record ) ->
  id = record[ 'id' ]
  unless id?
    throw new Error """
      unable to register record without ID:
      #{rpr record}"""
  #.......................................................................................................
  unless ( type = record[ '~label' ] )?
    throw new Error """
      unable to register untyped (= unlabelled) record:
      #{rpr record}"""
  #.......................................................................................................
  sub_registry = registry[ type ]
  throw new Error "unable to locate registry for GTFS type #{rpr type}" unless sub_registry?
  if ( dupe = sub_registry[ id ] )? and dupe isnt record
    throw new Error """already registered:
      #{rpr dupe}
      #{rpr record}"""
  #.......................................................................................................
  sub_registry[ id ] = record
  return null
