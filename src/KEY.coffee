


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
    id:           gtfs/stoptime/876
    stop-id:      gtfs/stop/123
    trip-id:      gtfs/trip/456
    ...

  stop:
    id:           gtfs/stop/123
    name:         Bayerischer+Platz
    ...

  trip:
    id:           gtfs/trip/456
    route-id:     gtfs/route/777
    service-id:   gtfs/service/888


gtfs/stoptime/876
gtfs/stoptime/876/stop-id:gtfs/stop/123
gtfs/stoptime/876/trip-id:gtfs/trip/456
gtfs/stop/123
gtfs/stop/123/name:Bayerischer+Platz
gtfs/trip/456


############################################################################################################

gtfs:
  stoptime:
    id:              gtfs/stoptime/876
    gtfs/stop/:      gtfs/stop/123
    gtfs/trip/:      gtfs/trip/456
    ...
    arr:              15:38
    dep:              15:38


  stop:
    id:              gtfs/stop/123
    name:             Bayerischer+Platz
    ...

  trip:
    id:              gtfs/trip/456
    gtfs/route/:     gtfs/route/777
    gtfs/service/:   gtfs/service/888

  route:
    id:              gtfs/route/777
    name:             U4

$ . | realm / type / idn
$ : | realm / type / idn | name | value
$ ^ | realm₀ / type₀ / idn₀|>realm₁ / type₁ / idn₁

$.|gtfs/route/777
$.|gtfs/stop/123
$.|gtfs/stoptime/876
$.|gtfs/trip/456

$:|gtfs/route/777|name|U4
$:|gtfs/stop/123|name|Bayerischer+Platz
$:|gtfs/stoptime/876|arr|15%2538
$:|gtfs/stoptime/876|dep|15%2538

$^|gtfs/stoptime/876|gtfs/stop/123
$^|gtfs/stoptime/876|gtfs/trip/456
$^|gtfs/trip/456|gtfs/route/777
$^|gtfs/trip/456|gtfs/service/888


  gtfs/stoptime/876|-1>gtfs/trip/456
+                       gtfs/trip/456|-1>gtfs/route/777
----------------------------------------------------------------
= gtfs/stoptime/876|-2>gtfs/route/777
+                       gtfs/route/777|name:U4
----------------------------------------------------------------
= gtfs/stoptime/876|=gtfs/route/name:U4

# or

= gtfs/stoptime/876|=gtfs/route|name:U4

# or

= gtfs/stoptime/876|=2>gtfs/route|name:U4|777



  gtfs/stoptime/876|-1>gtfs/trip/456
                        gtfs/trip/456|-1>gtfs/service/888
----------------------------------------------------------------
= gtfs/stoptime/876|-2>gtfs/service/888

============================================================================================================

% : | realm / type   | name | value | idn
% ^ | realm₀ / type₀ | n | realm₁ / type₁ / idn₁ | idn₀

%:|gtfs/route|name|U4|777
%:|gtfs/stoptime|arr|15%2538|876
%:|gtfs/stoptime|dep|15%2538|876
%:|gtfs/stop|name|Bayerischer+Platz|123
%^|gtfs/stoptime|1|gtfs/stop/123|876
%^|gtfs/stoptime|1|gtfs/trip/456|876
%^|gtfs/stoptime|2|gtfs/route/777|876
%^|gtfs/stoptime|2|gtfs/service/888|876
%^|gtfs/trip|gtfs/route/777|456
%^|gtfs/trip|gtfs/service/888|456


realm
type
name
value
idn

joiner      |
%
escape_chr
=
>
:
^


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
options                   = ( require '../../options' )[ 'keys' ]


############################################################################################################
# WRITERS
#-----------------------------------------------------------------------------------------------------------
@new_route = ( realm, type ) ->
  router = options[ 'router' ]
  return ( @esc realm ) + router + ( @esc type )

#-----------------------------------------------------------------------------------------------------------
@new_id = ( realm, type, idn ) ->
  router = options[ 'router' ]
  return ( @new_route realm, type ) + router + ( @esc idn )

#-----------------------------------------------------------------------------------------------------------
@new_node = ( realm, type, idn ) ->
  joiner = options[ 'joiner' ]
  return options[ 'primary' ] + options[ 'node' ] + joiner + ( @new_id realm, type, idn )

#-----------------------------------------------------------------------------------------------------------
@new_facet_pair = ( realm, type, idn, name, value ) ->
  return [
    ( @new_facet            realm, type, idn, name, value ),
    ( @new_secondary_facet  realm, type, idn, name, value ), ]

#-----------------------------------------------------------------------------------------------------------
@new_facet = ( realm, type, idn, name, value ) ->
  joiner = options[ 'joiner' ]
  return options[ 'primary' ]     \
    + options[ 'facet' ]          \
    + joiner                          \
    + ( @new_id realm, type, idn )    \
    + joiner                          \
    + ( @esc name )                   \
    + joiner                          \
    + ( @esc value )

#-----------------------------------------------------------------------------------------------------------
@new_secondary_facet = ( realm, type, idn, name, value ) ->
  joiner = options[ 'joiner' ]
  return options[ 'secondary' ]   \
    + options[ 'facet' ]          \
    + joiner                          \
    + ( @new_route realm, type )      \
    + joiner                          \
    + ( @esc name )                   \
    + joiner                          \
    + ( @esc value )                  \
    + joiner                          \
    + ( @esc idn )

#-----------------------------------------------------------------------------------------------------------
@new_link_pair = ( realm_0, type_0, idn_0, realm_1, type_1, idn_1 ) ->
  return [
    ( @new_link           realm_0, type_0, idn_0, realm_1, type_1, idn_1 ),
    ( @new_secondary_link realm_0, type_0, idn_0, realm_1, type_1, idn_1 ), ]

#-----------------------------------------------------------------------------------------------------------
@new_link = ( realm_0, type_0, idn_0, realm_1, type_1, idn_1 ) ->
  joiner = options[ 'joiner' ]
  return options[ 'primary' ]           \
    + options[ 'link' ]                 \
    + joiner                                \
    + ( @new_id realm_0, type_0, idn_0 )    \
    + joiner                                \
    + ( @new_id realm_1, type_1, idn_1 )

#-----------------------------------------------------------------------------------------------------------
@new_secondary_link = ( realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance = 1 ) ->
  joiner = options[ 'joiner' ]
  return options[ 'secondary' ]         \
    + options[ 'link' ]                 \
    + joiner                                \
    + ( @new_route realm_0, type_0 )        \
    + joiner                                \
    + distance                              \
    + joiner                                \
    + ( @new_id realm_1, type_1, idn_1 )    \
    + joiner                                \
    + ( @esc idn_0 )


############################################################################################################
# READERS
#-----------------------------------------------------------------------------------------------------------
@read = ( key ) ->
  [ [ layer, type ], fields... ] = key.split options[ 'joiner' ]
  #.........................................................................................................
  switch layer
    when options[ 'primary' ]
      switch type
        when options[ 'node'  ] then return @_read_primary_node    fields...
        when options[ 'facet' ] then return @_read_primary_facet   fields...
        when options[ 'link'  ] then return @_read_primary_link    fields...
        else throw new Error "unknown type mark #{rpr type}"
    #.......................................................................................................
    when options[ 'secondary' ]
      switch type
        # when options[ 'node'  ] then return @_read_secondary_node  fields...
        when options[ 'facet' ] then return @_read_secondary_facet fields...
        when options[ 'link'  ] then return @_read_secondary_link  fields...
        else throw new Error "unknown type mark #{rpr type}"
    #.......................................................................................................
    else throw new Error "unknown layer mark #{rpr layer}"

#-----------------------------------------------------------------------------------------------------------
@_read_primary_node = ( id ) ->
  return level: 'primary', type: 'node', id: id

#-----------------------------------------------------------------------------------------------------------
@_read_primary_facet = ( id, name, value ) ->
  return level: 'primary', type: 'facet', id: id, name: name, value: value

#-----------------------------------------------------------------------------------------------------------
@_read_primary_link = ( id_0, id_1 ) ->
  return level: 'primary', type: 'link', id: id_0, target: id_1, distance: 1

#-----------------------------------------------------------------------------------------------------------
@_read_secondary_facet = ( route, name, value, idn ) ->
  return level: 'secondary', type: 'facet', id: route + options[ 'router' ] + idn, name: name, value: value

#-----------------------------------------------------------------------------------------------------------
@_read_secondary_link = ( route_0, distance, id_1, idn_0 ) ->
  id_0 = route_0 + options[ 'router' ] + idn_0
  return level: 'primary', type: 'link', id: id_0, target: id_1, distance: parseInt distance, 10


############################################################################################################
# HELPERS
#-----------------------------------------------------------------------------------------------------------
@esc = ( text ) ->
  R = text
  R = R.replace /%/g,   '%25'
  R = R.replace /\|/g,  '%7C'
  # R = R.replace /:/g,   '%3A'
  R = R.replace /\//g,  '%2F'
  return R


############################################################################################################
unless module.parent?
  help @new_id                      'gtfs', 'stop', '123'
  help @new_node                    'gtfs', 'stop', '123'
  help @new_facet                   'gtfs', 'stop', '123', 'name', 'Bayerischer Platz'
  help @new_secondary_facet         'gtfs', 'stop', '123', 'name', 'Bayerischer Platz'
  help @new_facet_pair              'gtfs', 'stop', '123', 'name', 'Bayerischer Platz'
  help @new_link                    'gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'
  help @new_secondary_link          'gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'
  help @new_link_pair               'gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'
  help @read @new_node              'gtfs', 'stop', '123'
  help @read @new_facet             'gtfs', 'stop', '123', 'name', 'Bayerischer Platz'
  help @read @new_secondary_facet   'gtfs', 'stop', '123', 'name', 'Bayerischer Platz'
  help @read @new_link              'gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'
  help @read @new_secondary_link    'gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'


  # levelup = require 'level'
  # REGISTRY  = require '../REGISTRY'
  # db = levelup '/tmp/test.db'#, valueEncoding: 'binary'
  # value = new Buffer 0
  # value = null
  # value = true
  # value = 1
  # db.put 'mykey', value, ( error ) ->
  #   throw error if error?
  # REGISTRY.flush db, ( error ) ->
  #   throw error if error?
  #   db.get 'mykey', ( error, P... ) ->
  #     throw error if error?
  #     info P




