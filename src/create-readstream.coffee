

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
badge                     = 'timetable/main'
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
ProgressBar               = require 'progress'

#-----------------------------------------------------------------------------------------------------------
module.exports = create_readstream = ( route ) ->
  #.........................................................................................................
  switch type = TYPES.type_of route
    when 'text'
      R = njs_fs.createReadStream route
    when 'list'
      routes          = route
      CombinedStream  = require 'combined-stream'
      R               = CombinedStream.create()
      for partial_route in routes
        R.append njs_fs.createReadStream partial_route
    else
      throw new Error "unable to create readstream for argument of type #{rpr type}"
  #.........................................................................................................
  size = get_filesize route
  debug size
  options   =
    width:      50
    total:      16434
    complete:   '#'
    incomplete: '—'
  format    = '  reading  :percent | :bar | :elapseds :etas'
  bar       = new ProgressBar format, options
  R.on 'data', ( data ) ->
    bar.tick data.length
    if bar.complete
      info 'complete'
  return R

#-----------------------------------------------------------------------------------------------------------
get_filesize = ( route ) ->
  return ( njs_fs.statSync route ).size if ( TYPES.type_of route ) is 'text'
  R = 0
  R += get_filesize partial_route for partial_route in route
  return R
