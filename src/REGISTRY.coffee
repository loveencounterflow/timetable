


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
  #.........................................................................................................
  meta_value            = '1'
  entries               = []
  ### TAINT kludge, must be changed in GTFS reader ###
  [ realm, type, idn, ] = id.split '/'
  route                 = KEY.new_route realm, type
  key                   = KEY.new_node  realm, type, idn
  entries.push [ key, JSON.stringify record, ]
  #.........................................................................................................
  for name, value of record
    ### TAINT make configurable ###
    continue if name is 'id'
    continue if name is 'gtfs-id'
    continue if name is 'gtfs-type'
    facet_route = KEY.new_route realm, type, name
    #.....................................................................................................
    if ( has_index = indexes[ 'direct' ]?[ 'facet' ] )?
      has_primary   = has_index[ 'primary'   ]?[ facet_route ] ? false
      has_secondary = has_index[ 'secondary' ]?[ facet_route ] ? false
      if has_primary or has_secondary
        if has_primary
          key = KEY.new_facet           realm, type, idn, name, value
          entries.push [ key, meta_value, ]
        if has_secondary
          key = KEY.new_secondary_facet realm, type, idn, name, value
          entries.push [ key, meta_value, ]
    #.....................................................................................................
    if ( has_index = indexes[ 'direct' ]?[ 'link' ] )?
      has_primary   = has_index[ 'primary'   ]?[ facet_route ] ? false
      has_secondary = has_index[ 'secondary' ]?[ facet_route ] ? false
      if has_primary or has_secondary
        ### TAINT inefficiently first splitting, then joining ###
        id_1                        = value
        [ realm_1, type_1, idn_1, ] = KEY.split_id id_1
        if has_primary
          key = KEY.new_link           realm, type, idn, realm_1, type_1, idn_1, 0
          entries.push [ key, meta_value, ]
        if has_secondary
          key = KEY.new_secondary_link realm, type, idn, realm_1, type_1, idn_1, 0
          entries.push [ key, meta_value, ]
  #.........................................................................................................
  tasks = ( { type: 'put', key: key, value: value } for [ key, value, ] in entries )
  registry.batch tasks, ( error ) =>
    handler if error? then error else null

#-----------------------------------------------------------------------------------------------------------
@$register_2 = ( registry ) ->
  return $ ( record, handler ) =>
    @register_2 registry, record, ( error ) =>
      return handler error if error?
      handler null, record

#-----------------------------------------------------------------------------------------------------------
@register_deferred_properties = ( registry, handler ) ->
  ### TAINT simplify ###
  ### possible to rewrite in terms of a pipe? ###
  count = 0
  for entry_type, type_index of indexes[ 'inferred' ]
    for level, level_index of type_index
      for [ source_selector, target_facet_name, ] in level_index
        query =
          gte:      source_selector
          lte:      KEY.lte_from_gte source_selector
        registry.createKeyStream query
          .on 'data', ( source_key ) =>
            # [ target_realm, target_type, target_idn ] = KEY.split_id ( KEY.read source_key )[ 'target' ]
            # target_node_key = KEY.new_node target_realm, target_type, target_idn
            source_entry    = KEY.read source_key
            ### TAINT specific to links ###
            source_node_id  = source_entry[ 'id' ]
            proxy_node_id   = source_entry[ 'target' ]
            ### TAINT inefficiently splitting and joining key ###
            [ source_realm
              source_type
              source_idn   ] = KEY.split_id source_node_id
            ### TAINT inefficiently splitting and joining key ###
            [ proxy_realm
              proxy_type
              proxy_idn   ] = KEY.split_id proxy_node_id
            proxy_node_key  = KEY.new_node proxy_realm, proxy_type, proxy_idn
            registry.get proxy_node_key, ( error, proxy_node ) =>
              return handler error if error?
              ### TAINT why is this not done automatically? ###
              proxy_node      = JSON.parse proxy_node
              target_value    = proxy_node[ target_facet_name ]
              ### TAINT kludge ###
              unless target_value?
                return handler new Error "facet #{rpr target_facet_name} not defined in #{rpr target_node}"
              # help  rpr source_entry
              # debug rpr proxy_node
              # info  target_facet_name, target_value
              # info  source_realm, source_type, source_idn
              switch level
                when 'secondary'
                  switch entry_type
                    when 'link'
                      ### TAINT inefficiently splitting and joining key ###
                      [ target_realm
                        target_type
                        target_idn  ] = KEY.split_id target_value
                      distance        = source_entry[ 'distance' ] + 1
                      key             = KEY.new_secondary_link source_realm, source_type, source_idn, \
                                                               target_realm, target_type, target_idn, distance
                      registry.put key, '1', ( error ) =>
                        return handler error if error?
                        whisper count if count % 1000 is 0
                        count += 1
                    else
                      throw 'XXX'
                else
                  throw 'XXX'
              # registry.put
          .on 'error', ( error ) -> return handler error
          .on 'end', -> return handler null

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

############################################################################################################
unless module.parent?
  registry = @new_registry()
  @register_deferred_properties registry, ( error, data ) ->
    throw error if error?
    help data

