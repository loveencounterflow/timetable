


############################################################################################################
njs_fs                    = require 'fs'
#...........................................................................................................
TYPES                     = require 'coffeenode-types'
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
indexes                   = options[ 'data' ][ 'indexes' ]
new_db                    = require 'level'
#...........................................................................................................
ASYNC                     = require 'async'
#...........................................................................................................
P                         = require 'pipedreams'
$                         = P.$.bind P
KEY                       = require './KEY'



#-----------------------------------------------------------------------------------------------------------
test_folder_exists = ( route ) ->
  return false unless njs_fs.existsSync route
  is_folder = ( njs_fs.statSync route ).isDirectory()
  throw new Error "route exists but is not a folder: #{route}" unless is_folder
  return true

#-----------------------------------------------------------------------------------------------------------
@new_registry = ( route ) ->
  route ?= options[ 'levelup' ][ 'route' ]
  return new_db route, options[ 'levelup' ][ 'new' ]

#-----------------------------------------------------------------------------------------------------------
@_new_registry = ( route ) ->
  route        ?= options[ 'levelup' ][ 'route' ]
  folder_exists = test_folder_exists route
  registry      = @new_registry route
  return [ folder_exists, registry, ]

#-----------------------------------------------------------------------------------------------------------
@close = ( registry, handler ) ->
  registry.close ( error ) =>
    help 'registry closed'
    handler error

#-----------------------------------------------------------------------------------------------------------
@flush = ( registry, handler ) ->
  registry.close ( error ) =>
    return handler error if error?
    registry.open ( error ) =>
      help 'registry flushed'
      handler error, registry

#-----------------------------------------------------------------------------------------------------------
@register = ( registry, record, handler ) ->
  id = record[ 'id' ]
  unless id?
    throw new Error """
      unable to register record without ID:
      #{rpr record}"""
  ### Records whose only attribute is the ID field are replaced by `1`: ###
  value = if ( Object.keys record ).length is 1 then 1 else record
  registry.put id, value, ( error ) =>
    return handler error if error?
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$register = ( registry ) ->
  return $ ( record, handler ) =>
    @register registry, record, ( error ) =>
      return handler error if error?
      handler null, record

#-----------------------------------------------------------------------------------------------------------
@register_2 = ( registry, record, handler ) ->
  ### TAINT kludge, change to using strings as records ###
  id = record[ 'id' ]
  unless id?
    throw new Error """
      unable to register record without ID:
      #{rpr record}"""
  ### TAINT kludge, must be changed in GTFS reader ###
  [ realm, type, idn, ] = id.split '/'
  route       = KEY.new_route realm, type
  meta_value  = '1'
  tasks       = []
  for name, value of record
    continue if name is 'id'
    continue if name is 'gtfs-id'
    continue if name is 'gtfs-type'
    value = rpr value unless TYPES.isa_text value
    keys  = []
    ### TAINT must apply escaping or use KEY method ###
    route = [ realm, type, name ].join options[ 'keys' ][ 'slash' ]
    #.......................................................................................................
    if /-id$/.test name
      idn_1                   = value
      [ realm_1, type_1, _, ] = name.split '-'
      keys.push KEY.new_link           realm, type, idn, realm_1, type_1, idn_1, 0
      keys.push KEY.new_secondary_link realm, type, idn, realm_1, type_1, idn_1, 0 if indexes[ route ]
    #.......................................................................................................
    else
      keys.push KEY.new_facet           realm, type, idn, name, value
      keys.push KEY.new_secondary_facet realm, type, idn, name, value if indexes[ route ]
    #.......................................................................................................
    for key in keys
      do ( key ) ->
        tasks.push ( handler ) =>
          registry.put key, meta_value, ( error ) =>
            handler if error? then error else null
  #.........................................................................................................
  ASYNC.parallel tasks, ( error ) =>
    handler if error? then error else null

#-----------------------------------------------------------------------------------------------------------
@$register_2 = ( registry ) ->
  return $ ( record, handler ) =>
    @register_2 registry, record, ( error ) =>
      return handler error if error?
      handler null, record


  # #.......................................................................................................
  # gtfs_type = record[ '%gtfs-type' ]
  # unless gtfs_type?
  #   throw new Error """
  #     unable to register record without GTFS type:
  #     #{rpr record}"""
  # #.......................................................................................................
  # sub_registry = registry[ '%gtfs' ]?[ gtfs_type ]
  # throw new Error "unable to locate registry for GTFS type #{rpr gtfs_type}" unless sub_registry?
  # if ( dupe = sub_registry[ gtfs_id ] )? # and dupe isnt record
  #   throw new Error """already registered:
  #     #{rpr dupe}
  #     #{rpr record}"""
  # #.......................................................................................................
  # sub_registry[ gtfs_id ] = record
  # return null

# #-----------------------------------------------------------------------------------------------------------
# @register = ( registry, record ) ->
#   id = record[ 'id' ]
#   unless id?
#     throw new Error """
#       unable to register record without ID:
#       #{rpr record}"""
#   #.......................................................................................................
#   unless ( type = record[ '~label' ] )?
#     throw new Error """
#       unable to register untyped (= unlabelled) record:
#       #{rpr record}"""
#   #.......................................................................................................
#   sub_registry = registry[ type ]
#   throw new Error "unable to locate registry for GTFS type #{rpr type}" unless sub_registry?
#   if ( dupe = sub_registry[ id ] )? and dupe isnt record
#     throw new Error """already registered:
#       #{rpr dupe}
#       #{rpr record}"""
#   #.......................................................................................................
#   sub_registry[ id ] = record
#   return null
