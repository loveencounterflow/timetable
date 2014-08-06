

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
badge                     = 'timetable/TRANSFORMERS'
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
transform_stream          = require 'stream-transform'
options                   = require '../options'



############################################################################################################
# GENERIC METHODS
#-----------------------------------------------------------------------------------------------------------
@as_transformer = ( method ) -> transform_stream method, options[ 'stream-transform' ]

#-----------------------------------------------------------------------------------------------------------
@$as_pods = ->
  record_idx  = -1
  field_names = null
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    # whisper record.join ''
    if ( record_idx += 1 ) is 0
      field_names = record
      return
    R = {}
    for field_value, field_idx in record
      field_name      = field_names[ field_idx ]
      R[ field_name ] = field_value
    handler null, R

#-----------------------------------------------------------------------------------------------------------
@$delete_prefix = ( prefix ) ->
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    for old_field_name, field_value of record
      continue unless TEXT.starts_with old_field_name, prefix
      new_field_name =  old_field_name.replace prefix, ''
      continue if new_field_name.length is 0
      ### TAINT should throw error ###
      continue if record[ new_field_name ]?
      record[ new_field_name ] = field_value
      delete record[ old_field_name ]
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$add_n4j_system_properties = ( isa, label ) ->
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    record[ '~isa'    ] = isa
    record[ '~label'  ] = label
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$dasherize_field_names = ->
  return @as_transformer ( record, handler ) =>
    for old_field_name of record
      new_field_name = old_field_name.replace /_/g, '-'
      continue if new_field_name is old_field_name
      @_rename record, old_field_name, new_field_name
    #.......................................................................................................
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@_rename = ( record, old_field_name, new_field_name ) ->
  @_copy record, old_field_name, new_field_name, 'rename'
  delete record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@_copy = ( record, old_field_name, new_field_name, action ) ->
  #.........................................................................................................
  if record[ old_field_name ] is undefined
    error = new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that there is no field #{rpr old_field_name} in
      #{rpr record}"""
    error[ 'code' ] = 'no such field'
    throw error
  #.........................................................................................................
  if record[ new_field_name ] isnt undefined
    throw new Error """
      when trying to #{action} field #{rpr old_field_name} to #{rpr new_field_name}
      found that field #{rpr new_field_name} already present in
      #{rpr record}"""
    error[ 'code' ] = 'duplicate field'
    throw error
  #.........................................................................................................
  record[ new_field_name ] = record[ old_field_name ]
  return record

#-----------------------------------------------------------------------------------------------------------
@$rename = ( old_field_name, new_field_name ) ->
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    handler null, @_rename record, old_field_name, new_field_name

#-----------------------------------------------------------------------------------------------------------
@$copy = ( old_field_name, new_field_name ) ->
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    handler null, @_copy record, old_field_name, new_field_name, 'copy'

#-----------------------------------------------------------------------------------------------------------
### TAINT gtfs-specific method shouldnt appear here ###
### TAINT consider sub-registries by node label ###
@$register = ( registry ) ->
  #.........................................................................................................
  return @as_transformer ( record, handler ) =>
    # whisper record
    old_registry  = registry[ 'old' ]?= {}
    new_registry  = registry[ 'new' ]?= {}
    gtfs_id       = record[ '%gtfs-id' ]
    id            = record[       'id' ]
    #.......................................................................................................
    if gtfs_id?
      if old_registry[ gtfs_id ]?
        throw new Error "already registered in `registry[ 'old' ]`: #{rpr record}"
      old_registry[ gtfs_id ] = record
    #.......................................................................................................
    if id?
      if new_registry[ id ]?
        throw new Error "already registered in `registry[ 'new' ]`: #{rpr record}"
      new_registry[ id ] = record
    #.......................................................................................................
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show_sample = ( input_stream ) ->
  ### TAINT may introduce a memory leak. ###
  records = []
  input_stream.once 'end', =>
    info rpr records[ Math.floor Math.random() * records.length ]
  return @as_transformer ( record, handler ) =>
    records.push record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$skip = ( limit = 1 ) ->
  count = 0
  return @as_transformer ( record, handler ) =>
    count += 1
    return handler null, null if count > limit
    handler null, record #, { foo: 42, id: record[ 'id' ] + 'XXX' }

#-----------------------------------------------------------------------------------------------------------
@$show = ->
  return @as_transformer ( record, handler ) =>
    info rpr record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
@$show_and_quit = ->
  return @as_transformer ( record, handler ) =>
    info rpr record
    warn 'aborting from `TRANSFORMERS.show_and_quit`'
    setImmediate -> process.exit()
    handler null, record
