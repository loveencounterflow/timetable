
#-----------------------------------------------------------------------------------------------------------
fs                        = require 'fs'
transform_stream          = require 'stream-transform'
log                       = console.log
as_transformer            = ( method ) -> transform_stream method, parallel: 11
# _new_csv_parser           = require 'csv-parse'
# new_csv_parser            = -> _new_csv_parser delimiter: ','

#-----------------------------------------------------------------------------------------------------------
$count = ( input_stream, title ) ->
  count = 0
  #.........................................................................................................
  input_stream.on 'end', ->
    log ( title ? 'Count' ) + ':', count
  #.........................................................................................................
  return as_transformer ( record, handler ) =>
    count += 1
    handler null, record

#-----------------------------------------------------------------------------------------------------------
$show = ->
  return as_transformer ( record, handler ) =>
    log record
    handler null, record

#-----------------------------------------------------------------------------------------------------------
read_trips = ( route, handler ) ->
  # parser      = new_csv_parser()
  input       = fs.createReadStream route
  #.........................................................................................................
  input.on 'end', ->
    log 'ok: trips'
    return handler null
  input.setMaxListeners 100 # <<<<<<
  #.........................................................................................................
  # input.pipe parser
  input.pipe $count input, 'trips A'
    .pipe $count    input, 'trips B'
    .pipe $count    input, 'trips C'
    .pipe $count    input, 'trips D'
    .pipe $count    input, 'trips E'
    .pipe $count    input, 'trips F'
    .pipe $count    input, 'trips G'
    .pipe $count    input, 'trips H'
    .pipe $count    input, 'trips I'
    .pipe $count    input, 'trips J'
    .pipe $count    input, 'trips K'
    .pipe $count    input, 'trips M'
    .pipe $count    input, 'trips N'
    .pipe $count    input, 'trips O'
    .pipe $count    input, 'trips P'
    .pipe $count    input, 'trips Q'
    .pipe $count    input, 'trips R'
    .pipe $count    input, 'trips R'
    .pipe $count    input, 'trips S'
    .pipe $count    input, 'trips T'
    .pipe $count    input, 'trips U'
    .pipe $count    input, 'trips V'
    .pipe $count    input, 'trips W'
    .pipe $count    input, 'trips X'
    .pipe $count    input, 'trips Y'
    .pipe $count    input, 'trips Z'
    # .pipe $show
  #.........................................................................................................
  return null

# route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/trips.txt'
route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/agency.txt'
read_trips route, ( error ) ->
  throw error if error?
  log 'ok'


