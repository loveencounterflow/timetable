// Generated by CoffeeScript 1.7.1

/*

gtfs:
  stoptime:
    id:               gtfs/stoptime/876
    stop-id:          gtfs/stop/123
    trip-id:          gtfs/trip/456
    ...
    arr:              15:38
    dep:              15:38


  stop:
    id:               gtfs/stop/123
    name:             Bayerischer+Platz
    ...

  trip:
    id:               gtfs/trip/456
    route-id:         gtfs/route/777
    service-id:       gtfs/service/888

  route:
    id:               gtfs/route/777
    name:             U4

$ . | realm / type / idn
$ : | realm / type / idn | name | value
$ ^ | realm₀ / type₀ / idn₀|>realm₁ / type₁ / idn₁


$:|gtfs/route/777|0|name|U4
$:|gtfs/stop/123|0|name|Bayerischer+Platz
$:|gtfs/stoptime/876|0|arr|15%2538
$:|gtfs/stoptime/876|0|dep|15%2538
$^|gtfs/stoptime/876|0|gtfs/stop/123
$^|gtfs/stoptime/876|0|gtfs/trip/456
$^|gtfs/trip/456|0|gtfs/route/777
$^|gtfs/trip/456|0|gtfs/service/888


  $^|gtfs/stoptime/876|gtfs/trip/456
+                   $^|gtfs/trip/456|gtfs/route/777
----------------------------------------------------------------
= %^|gtfs/stoptime| 2               |gtfs/route/777|876
+                                 $:|gtfs/route/777|name|U4
----------------------------------------------------------------
= %:|gtfs/stoptime/876              |gtfs/route/    name|U4

 * or

= gtfs/stoptime/876|=gtfs/route|name:U4

 * or

= gtfs/stoptime/876|=2>gtfs/route|name:U4|777



  gtfs/stoptime/876|-1>gtfs/trip/456
                        gtfs/trip/456|-1>gtfs/service/888
----------------------------------------------------------------
= gtfs/stoptime/876|-2>gtfs/service/888

============================================================================================================

% : | realm / type   | name | value | idn
% ^ | realm₀ / type₀ | n | realm₁ / type₁ / idn₁ | idn₀

%:|gtfs/route|0|name|U4|777
%:|gtfs/stoptime|0|arr|15%2538|876
%:|gtfs/stoptime|0|dep|15%2538|876
%:|gtfs/stop|0|name|Bayerischer+Platz|123
%^|gtfs/stoptime|0|gtfs/stop/123|876
%^|gtfs/stoptime|0|gtfs/trip/456|876
%^|gtfs/stoptime|1|gtfs/route/777|876
%^|gtfs/stoptime|1|gtfs/service/888|876
%^|gtfs/trip|0|gtfs/route/777|456
%^|gtfs/trip|0|gtfs/service/888|456


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
 */

(function() {
  var TRM, TYPES, alert, badge, debug, echo, help, info, log, njs_fs, options, rainbow, rpr, urge, warn, whisper,
    __slice = [].slice;

  njs_fs = require('fs');

  TYPES = require('coffeenode-types');

  TRM = require('coffeenode-trm');

  rpr = TRM.rpr.bind(TRM);

  badge = 'TIMETABLE/GTFS-READER';

  log = TRM.get_logger('plain', badge);

  info = TRM.get_logger('info', badge);

  whisper = TRM.get_logger('whisper', badge);

  alert = TRM.get_logger('alert', badge);

  debug = TRM.get_logger('debug', badge);

  warn = TRM.get_logger('warn', badge);

  help = TRM.get_logger('help', badge);

  urge = TRM.get_logger('urge', badge);

  echo = TRM.echo.bind(TRM);

  rainbow = TRM.rainbow.bind(TRM);

  options = (require('../options'))['keys'];

  this.new_route = function(realm, type, name) {
    var R, part;
    R = [realm, type];
    if (name != null) {
      R.push(name);
    }
    return ((function() {
      var _i, _len, _results;
      _results = [];
      for (_i = 0, _len = R.length; _i < _len; _i++) {
        part = R[_i];
        _results.push(this.esc(part));
      }
      return _results;
    }).call(this)).join(options['slash']);
  };

  this.new_id = function(realm, type, idn) {
    var slash;
    slash = options['slash'];
    return (this.new_route(realm, type)) + slash + (this.esc(idn));
  };

  this.new_node = function(realm, type, idn) {
    var joiner;
    joiner = options['joiner'];
    return options['primary'] + options['node'] + joiner + (this.new_id(realm, type, idn)) + joiner;
  };

  this.new_facet_pair = function(realm, type, idn, name, value, distance) {
    if (distance == null) {
      distance = 0;
    }
    return [this.new_facet(realm, type, idn, name, value, distance), this.new_secondary_facet(realm, type, idn, name, value, distance)];
  };

  this.new_facet = function(realm, type, idn, name, value, distance) {
    var joiner;
    if (distance == null) {
      distance = 0;
    }
    joiner = options['joiner'];
    return options['primary'] + options['facet'] + joiner + (this.new_id(realm, type, idn)) + joiner + distance + joiner + (this.esc(name)) + joiner + (this.esc(value)) + joiner;
  };

  this.new_secondary_facet = function(realm, type, idn, name, value, distance) {
    var joiner;
    if (distance == null) {
      distance = 0;
    }
    joiner = options['joiner'];
    return options['secondary'] + options['facet'] + joiner + (this.new_route(realm, type)) + joiner + distance + joiner + (this.esc(name)) + joiner + (this.esc(value)) + joiner + (this.esc(idn)) + joiner;
  };

  this.new_link_pair = function(realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance) {
    if (distance == null) {
      distance = 0;
    }
    return [this.new_link(realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance), this.new_secondary_link(realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance)];
  };

  this.new_link = function(realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance) {
    var joiner;
    if (distance == null) {
      distance = 0;
    }
    joiner = options['joiner'];
    return options['primary'] + options['link'] + joiner + (this.new_id(realm_0, type_0, idn_0)) + joiner + distance + joiner + (this.new_id(realm_1, type_1, idn_1)) + joiner;
  };

  this.new_secondary_link = function(realm_0, type_0, idn_0, realm_1, type_1, idn_1, distance) {
    var joiner;
    if (distance == null) {
      distance = 0;
    }
    joiner = options['joiner'];
    return options['secondary'] + options['link'] + joiner + (this.new_route(realm_0, type_0)) + joiner + distance + joiner + (this.new_id(realm_1, type_1, idn_1)) + joiner + (this.esc(idn_0)) + joiner;
  };

  this.read = function(key) {
    var R, fields, layer, type, _ref, _ref1;
    _ref = key.split(options['joiner']), (_ref1 = _ref[0], layer = _ref1[0], type = _ref1[1]), fields = 2 <= _ref.length ? __slice.call(_ref, 1) : [];
    switch (layer) {
      case options['primary']:
        switch (type) {
          case options['node']:
            R = this._read_primary_node.apply(this, fields);
            break;
          case options['facet']:
            R = this._read_primary_facet.apply(this, fields);
            break;
          case options['link']:
            R = this._read_primary_link.apply(this, fields);
            break;
          default:
            throw new Error("unknown type mark " + (rpr(type)));
        }
        break;
      case options['secondary']:
        switch (type) {
          case options['facet']:
            R = this._read_secondary_facet.apply(this, fields);
            break;
          case options['link']:
            R = this._read_secondary_link.apply(this, fields);
            break;
          default:
            throw new Error("unknown type mark " + (rpr(type)));
        }
        break;
      default:
        throw new Error("unknown layer mark " + (rpr(layer)));
    }
    R['key'] = key;
    return R;
  };

  this._read_primary_node = function(id) {
    var R;
    R = {
      level: 'primary',
      type: 'node',
      id: id
    };
    return R;
  };

  this._read_primary_facet = function(id, distance, name, value) {
    var R;
    R = {
      level: 'primary',
      type: 'facet',
      id: id,
      name: name,
      value: value,
      distance: parseInt(distance, 10)
    };
    return R;
  };

  this._read_primary_link = function(id_0, distance, id_1) {
    var R;
    R = {
      level: 'primary',
      type: 'link',
      id: id_0,
      target: id_1,
      distance: parseInt(distance, 10)
    };
    return R;
  };

  this._read_secondary_facet = function(route, distance, name, value, idn) {
    var R;
    R = {
      level: 'secondary',
      type: 'facet',
      id: route + options['slash'] + idn,
      name: name,
      value: value,
      distance: parseInt(distance, 10)
    };
    return R;
  };

  this._read_secondary_link = function(route_0, distance, id_1, idn_0) {
    var R, id_0;
    id_0 = route_0 + options['slash'] + idn_0;
    R = {
      level: 'secondary',
      type: 'link',
      id: id_0,
      target: id_1,
      distance: parseInt(distance, 10)
    };
    return R;
  };

  this.infer = function(key_0, key_1) {
    var id_1, id_2, info_0, info_1, type_0, type_1;
    info_0 = TYPES.isa_text(key_0) ? this.read(key_0) : key_0;
    info_1 = TYPES.isa_text(key_1) ? this.read(key_1) : key_1;
    if ((type_0 = info_0['type']) === 'link') {
      if ((id_1 = info_0['target']) !== (id_2 = info_1['id'])) {
        throw new Error("unable to infer link from " + (rpr(info_0['key'])) + " and " + (rpr(info_1['key'])));
      }
      switch (type_1 = info_1['type']) {
        case 'link':
          return this._infer_link(info_0, info_1);
        case 'facet':
          return this._infer_facet(info_0, info_1);
      }
    }
    throw new Error("expected a link plus a link or a facet, got a " + type_0 + " and a " + type_1);
  };

  this._infer_facet = function(link, facet) {
    var distance, facet_idn, facet_realm, facet_type, link_idn, link_realm, link_type, name, slash, value, _ref, _ref1;
    _ref = this.split_id(link['id']), link_realm = _ref[0], link_type = _ref[1], link_idn = _ref[2];
    _ref1 = this.split_id(link['id']), facet_realm = _ref1[0], facet_type = _ref1[1], facet_idn = _ref1[2];

    /* TAINT route not distinct from ID? */

    /* TAINT should slashes in name be escaped? */

    /* TAINT what happens when we infer from an inferred facet? do all the escapes get re-escaped? */

    /* TAINT use module method */
    slash = options['slash'];
    name = (this.esc(facet_realm)) + slash + (this.esc(facet_type)) + slash + (this.esc(facet['name']));
    value = facet['value'];
    distance = link['distance'] + facet['distance'] + 1;
    return this.new_facet_pair(link_realm, link_type, link_idn, name, value, distance);
  };

  this._infer_link = function(link_0, link_1) {

    /*
      $^|gtfs/stoptime/876|0|gtfs/trip/456
    +                   $^|gtfs/trip/456|0|gtfs/route/777
    ----------------------------------------------------------------
    = $^|gtfs/stoptime/876|1|gtfs/route/777
    = %^|gtfs/stoptime|1|gtfs/route/777|876
     */
    var distance, idn_0, idn_2, realm_0, realm_2, type_0, type_2, _ref, _ref1;
    _ref = this.split_id(link_0['id']), realm_0 = _ref[0], type_0 = _ref[1], idn_0 = _ref[2];
    _ref1 = this.split_id(link_1['target']), realm_2 = _ref1[0], type_2 = _ref1[1], idn_2 = _ref1[2];
    distance = link_0['distance'] + link_1['distance'] + 1;
    return this.new_link_pair(realm_0, type_0, idn_0, realm_2, type_2, idn_2, distance);
  };

  this.esc = (function() {
    var d, escape, joiner_matcher, joiner_replacer, slash_matcher, slash_replacer;
    escape = function(text) {
      var R;
      R = text;
      R = R.replace(/([-()\[\]{}+?*.$\^|,:#<!\\])/g, '\\$1');
      R = R.replace(/\x08/g, '\\x08');
      return R;
    };
    joiner_matcher = new RegExp(escape(options['joiner']), 'g');
    slash_matcher = new RegExp(escape(options['slash']), 'g');
    joiner_replacer = ((function() {
      var _i, _len, _ref, _results;
      _ref = new Buffer(options['joiner']);
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        d = _ref[_i];
        _results.push('%' + d.toString(16));
      }
      return _results;
    })()).join('');
    slash_replacer = ((function() {
      var _i, _len, _ref, _results;
      _ref = new Buffer(options['slash']);
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        d = _ref[_i];
        _results.push('%' + d.toString(16));
      }
      return _results;
    })()).join('');
    return function(x) {
      var R;
      if (x === void 0) {
        throw new Error("value cannot be undefined");
      }
      R = TYPES.isa_text(x) ? x : rpr(x);
      R = R.replace(/%/g, '%25');
      R = R.replace(joiner_matcher, joiner_replacer);
      R = R.replace(slash_matcher, slash_replacer);
      return R;
    };
  })();

  this.split_id = function(id) {

    /* TAINT must unescape */
    var R, slash;
    R = id.split(slash = options['slash']);
    if (R.length !== 3) {
      throw new Error("expected three parts separated by " + (rpr(slash)) + ", got " + (rpr(id)));
    }
    if (!(R[0].length > 0)) {
      throw new Error("realm cannot be empty in " + (rpr(id)));
    }
    if (!(R[1].length > 0)) {
      throw new Error("type cannot be empty in " + (rpr(id)));
    }
    if (!(R[2].length > 0)) {
      throw new Error("IDN cannot be empty in " + (rpr(id)));
    }
    return R;
  };

  this._idn_from_id = function(id) {
    var match;
    match = id.replace(/^.+?([^\/]+)$/);
    if (match == null) {
      throw new Error("not a valid ID: " + (rpr(id)));
    }
    return match[1];
  };

  this.lte_from_gte = function(gte) {
    var R, length;
    length = Buffer.byteLength(gte);
    R = new Buffer(1 + length);
    R.write(gte);
    R[length] = 0xff;
    return R;
  };

  if (module.parent == null) {
    help(this.new_id('gtfs', 'stop', '123'));
    help(this.new_facet('gtfs', 'stop', '123', 'name', 1234));
    help(this.new_facet('gtfs', 'stop', '123', 'name', 'foo/bar|baz'));
    help(this.new_facet('gtfs', 'stop', '123', 'name', 'Bayerischer Platz'));
    help(this.new_secondary_facet('gtfs', 'stop', '123', 'name', 'Bayerischer Platz'));
    help(this.new_facet_pair('gtfs', 'stop', '123', 'name', 'Bayerischer Platz'));
    help(this.new_link('gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'));
    help(this.new_secondary_link('gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'));
    help(this.new_link_pair('gtfs', 'stoptime', '456', 'gtfs', 'stop', '123'));
    help(this.read(this.new_facet('gtfs', 'stop', '123', 'name', 'Bayerischer Platz')));
    help(this.read(this.new_secondary_facet('gtfs', 'stop', '123', 'name', 'Bayerischer Platz')));
    help(this.read(this.new_link('gtfs', 'stoptime', '456', 'gtfs', 'stop', '123')));
    help(this.read(this.new_secondary_link('gtfs', 'stoptime', '456', 'gtfs', 'stop', '123')));
    help(this.infer('$^|gtfs/stoptime/876|0|gtfs/trip/456', '$^|gtfs/trip/456|0|gtfs/route/777'));
    help(this.infer('$^|gtfs/stoptime/876|0|gtfs/trip/456', '%^|gtfs/trip|0|gtfs/route/777|456'));
    help(this.infer('$^|gtfs/trip/456|0|gtfs/stop/123', '$:|gtfs/stop/123|0|name|Bayerischer Platz'));
    help(this.infer('$^|gtfs/stoptime/876|1|gtfs/stop/123', '$:|gtfs/stop/123|0|name|Bayerischer Platz'));
  }

}).call(this);
