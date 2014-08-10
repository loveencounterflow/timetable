

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
badge                     = 'TIMETABLE/timetable-from-gtfs-data'
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
REGISTRY                  = require './REGISTRY'
P                         = require 'pipedreams'
$                         = P.$.bind P
#...........................................................................................................
options                   = require '../options'
DEV                       = options[ 'mode' ] is 'dev'

#-----------------------------------------------------------------------------------------------------------
@$register = ( registry ) ->
  return $ ( node, handler ) =>
    REGISTRY.register registry, node
    handler null, node

#-----------------------------------------------------------------------------------------------------------
### TAINT very Berlin-specific method, shouldnt appear here ###
@normalize_name = ( name ) ->
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
@$normalize_name = ( key ) ->
  return $ ( node, handler ) =>
    name = node[ key ]
    unless name?
      return handler new Error """
        unable to find key #{rpr key} in
        #{rpr node}"""
    node[ key ] = @normalize_name name
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

#-----------------------------------------------------------------------------------------------------------
@timetable_from_gtfs_data = ( gtfs_registry, handler ) ->

#-----------------------------------------------------------------------------------------------------------
@main = ( registry, handler ) ->
  # ratio = if DEV then 1 / 100 else 1
  input = P.read_values registry[ '%gtfs' ][ 'stops' ]
  #.........................................................................................................
  input
    # .pipe P.$sample                     ratio, headers: true, seed: 5 # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    .pipe @$normalize_name              'name'
    .pipe @$add_n4j_system_properties   'station'
    .pipe @$add_id()
    .pipe @$remove_gtfs_fields()
    .pipe @$register                    registry
    .pipe P.$collect_sample             input, 1, ( _, sample ) -> whisper 'stop', sample
    .on 'end', =>
      info 'ok: routes'
      return handler null




