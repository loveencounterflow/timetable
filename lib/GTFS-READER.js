// Generated by CoffeeScript 1.7.1
(function() {
  var $, ASYNC, DEV, KEY, P, REGISTRY, TEXT, TRM, TYPES, alert, badge, datasource_infos, debug, echo, global_data_limit, help, info, log, njs_fs, options, rainbow, registry, rpr, urge, warn, whisper, _ref, _ref1,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; },
    __slice = [].slice;

  njs_fs = require('fs');

  TYPES = require('coffeenode-types');

  TEXT = require('coffeenode-text');

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

  options = require('../options');

  global_data_limit = (_ref = (_ref1 = options['data']) != null ? _ref1['limit'] : void 0) != null ? _ref : Infinity;

  datasource_infos = (require('./get-datasource-infos'))();

  REGISTRY = require('./REGISTRY');

  KEY = require('./KEY');

  ASYNC = require('async');


  /* https://github.com/loveencounterflow/pipedreams */

  P = require('pipedreams');

  $ = P.$.bind(P);

  DEV = options['mode'] === 'dev';

  this.$set_id_from_gtfs_id = function() {
    return $((function(_this) {
      return function(record, handler) {
        var gtfs_id, gtfs_type;
        gtfs_id = record['gtfs-id'];
        gtfs_type = record['gtfs-type'];
        if (gtfs_id == null) {
          return handler(new Error("unable to register record without GTFS ID: " + (rpr(record))));
        }
        if (gtfs_type == null) {
          return handler(new Error("unable to register record without GTFS type: " + (rpr(record))));
        }
        record['id'] = "gtfs/" + gtfs_type + "/" + gtfs_id;
        return handler(null, record);
      };
    })(this));
  };

  this.$set_id = function(realm, type, name) {
    if (name == null) {
      name = 'id';
    }
    return $((function(_this) {
      return function(record, handler) {
        var idn;
        idn = record[name];
        if (idn == null) {
          return handler(new Error("unable to set ID without IDN: " + (rpr(record))));
        }
        if (__indexOf.call(idn, '/') >= 0) {
          return handler(new Error("illegal IDN in field " + (rpr(name)) + ": " + (rpr(record))));
        }
        record[name] = KEY.new_id(realm, type, idn);
        return handler(null, record);
      };
    })(this));
  };

  this.$convert_latlon = function() {
    return $((function(_this) {
      return function(record, handler) {
        record['lat'] = parseFloat(record['lat']);
        record['lon'] = parseFloat(record['lon']);
        return handler(null, record);
      };
    })(this));
  };

  this.$_XXXX_add_id_indexes = function() {
    var on_data;
    on_data = function(record) {
      var id, index_record, match, name, realm, ref_id, ref_realm, ref_type, type, _;
      realm = 'gtfs';
      type = record['gtfs-type'];
      id = record['gtfs-id'];
      for (name in record) {
        ref_id = record[name];
        match = name.match(/^gtfs-(.+?)-id$/);
        if (match == null) {
          continue;
        }
        _ = match[0], ref_type = match[1];
        ref_realm = 'gtfs';
        index_record = {
          'id': "%|" + realm + "/" + type + "|" + ref_realm + "/" + ref_type + "/" + ref_id + "|" + id
        };
        this.emit('data', index_record);
      }
      return this.emit('data', record);
    };
    return P.through(on_data, null);
  };

  this.$index_on = function() {
    var escape, names, on_data;
    names = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
    escape = this._escape_for_index;
    on_data = function(record) {
      var id, index_record, name, realm, type, value, value_type, _i, _len;
      realm = 'gtfs';
      type = record['gtfs-type'];
      id = record['gtfs-id'];
      for (_i = 0, _len = names.length; _i < _len; _i++) {
        name = names[_i];
        value = record[name];
        if (value == null) {
          continue;
        }
        if ((value_type = TYPES.type_of(value)) !== 'text') {
          throw new Error("building index from type " + (rpr(value_type)) + " not currently supported");
        }
        value = escape(value);
        index_record = {
          'id': "%|" + realm + "/" + type + "|" + name + ":" + value + "|" + id
        };
        this.emit('data', index_record);
      }
      return this.emit('data', record);
    };
    return P.through(on_data, null);
  };

  this._escape_for_index = function(text) {
    var R;
    R = text;
    R = R.replace(/%/g, '%25');
    R = R.replace(/\|/g, '%7C');
    R = R.replace(/:/g, '%3A');
    R = R.replace(/\//g, '%2F');
    return R;
  };

  this.$index_on_2 = function() {
    var escape, names, on_data;
    names = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
    escape = this._escape_for_index;
    on_data = function(record) {
      var id, index_record, name, realm, type, value, value_type, _i, _len;
      realm = 'gtfs';
      type = record['gtfs-type'];
      id = record['gtfs-id'];
      for (_i = 0, _len = names.length; _i < _len; _i++) {
        name = names[_i];
        value = record[name];
        if (value == null) {
          continue;
        }
        if ((value_type = TYPES.type_of(value)) !== 'text') {
          throw new Error("building index from type " + (rpr(value_type)) + " not currently supported");
        }
        value = escape(value);
        index_record = {
          'id': "%|" + realm + "/" + type + "|" + name + ":" + value + "|" + id
        };
        this.emit('data', index_record);
      }
      return this.emit('data', record);
    };
    return P.through(on_data, null);
  };

  this.read_agency = function(registry, route, handler) {
    var input;
    input = P.create_readstream(route, 'agency');
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$parse_csv()).pipe(this.$clean_agency_record()).pipe(P.$delete_prefix('agency_')).pipe(this.$clean_agency_id('id')).pipe(this.$set_id('gtfs', 'agency')).pipe(this.$clean_agency_record()).pipe(P.$dasherize_field_names()).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(4, function(_, sample) {
      return whisper('agency', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_agency_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['agency_phone'];
        delete record['agency_lang'];
        return handler(null, record);
      };
    })(this));
  };

  this.$clean_agency_id = function(name) {
    return $((function(_this) {
      return function(record, handler) {
        record[name] = record[name].replace(/[-_]*$/, '');
        return handler(null, record);
      };
    })(this));
  };


  /* TAINT name clash (filesystem route vs. GTFS route) */

  this.read_routes = function(registry, route, handler) {
    var input, ratio;
    ratio = DEV ? 1 / 10 : 1;
    input = P.create_readstream(route, 'routes');
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$sample(ratio, {
      headers: true,
      seed: 5
    })).pipe(P.$parse_csv()).pipe(this.$clean_routes_record()).pipe(P.$dasherize_field_names()).pipe(P.$rename('route-id', 'id')).pipe(P.$rename('agency-id', 'gtfs-agency-id')).pipe(this.$clean_agency_id('gtfs-agency-id')).pipe(this.$set_id('gtfs', 'agency', 'gtfs-agency-id')).pipe(P.$rename('route-short-name', 'name')).pipe(this.$set_id('gtfs', 'route')).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(8, function(_, sample) {
      return whisper('route', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_routes_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['route_long_name'];
        delete record['route_desc'];
        delete record['route_url'];
        delete record['route_color'];
        delete record['route_text_color'];
        return handler(null, record);
      };
    })(this));
  };

  this.read_calendar_dates = function(registry, route, handler) {
    var input, ratio;
    input = P.create_readstream(route, 'calendar_dates');
    ratio = DEV ? 1 / 100 : 1;
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$parse_csv()).pipe(P.$sample(ratio, {
      headers: true,
      seed: 5
    })).pipe(this.$clean_calendar_date_record()).pipe(P.$rename('service_id', 'id')).pipe(this.$set_id('gtfs', 'service')).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(4, function(_, sample) {
      return whisper('service', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_calendar_date_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['exception_type'];
        return handler(null, record);
      };
    })(this));
  };

  this.read_trips = function(registry, route, handler) {
    var input, ratio;
    input = P.create_readstream(route, 'trips');
    ratio = DEV ? 1 / 10000 : 1;
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$parse_csv()).pipe(P.$sample(ratio, {
      headers: true,
      seed: 5
    })).pipe(this.$clean_trip_record()).pipe(P.$delete_prefix('trip_')).pipe(P.$dasherize_field_names()).pipe(P.$rename('route-id', 'gtfs-route-id')).pipe(P.$rename('service-id', 'gtfs-service-id')).pipe(this.$set_id('gtfs', 'trip')).pipe(this.$set_id('gtfs', 'route', 'gtfs-route-id')).pipe(this.$set_id('gtfs', 'service', 'gtfs-service-id')).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(4, function(_, sample) {
      return whisper('trip', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_trip_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['trip_short_name'];
        delete record['direction_id'];
        delete record['block_id'];
        delete record['shape_id'];
        return handler(null, record);
      };
    })(this));
  };

  this.read_stop_times = function(registry, route, handler) {
    var input, ratio;
    input = P.create_readstream(route, 'stop_times');
    ratio = DEV ? 1 / 10000 : 1;
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$sample(ratio, {
      headers: true
    })).pipe(P.$parse_csv()).pipe(this.$clean_stop_times_record()).pipe(P.$set('gtfs-type', 'stop_times')).pipe(P.$dasherize_field_names()).pipe(P.$rename('trip-id', 'gtfs-trip-id')).pipe(P.$rename('stop-id', 'gtfs-stop-id')).pipe(P.$rename('arrival-time', 'arr')).pipe(P.$rename('departure-time', 'dep')).pipe(this.$add_stoptimes_gtfs_id()).pipe(this.$set_id('gtfs', 'trip', 'gtfs-trip-id')).pipe(this.$set_id('gtfs', 'stop', 'gtfs-stop-id')).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(5, function(_, sample) {
      return whisper('stop_time', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_stop_times_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['stop_headsign'];
        delete record['pickup_type'];
        delete record['drop_off_type'];
        delete record['shape_dist_traveled'];
        return handler(null, record);
      };
    })(this));
  };

  this.$add_stoptimes_gtfs_id = function() {
    var idx;
    idx = 0;
    return $((function(_this) {
      return function(record, handler) {
        record['id'] = KEY.new_id('gtfs', 'stoptime', "" + idx);
        idx += 1;
        return handler(null, record);
      };
    })(this));
  };

  this.read_stops = function(registry, route, handler) {
    var input;
    input = P.create_readstream(route, 'stops');
    input.pipe(P.$split()).pipe(P.$skip_empty()).pipe(P.$parse_csv()).pipe(this.$clean_stops_record()).pipe(P.$delete_prefix('stop_')).pipe(this.$set_id('gtfs', 'stop')).pipe(this.$convert_latlon()).pipe(REGISTRY.$register_2(registry)).pipe(P.$collect_sample(4, function(_, sample) {
      return whisper('stop', sample);
    })).pipe(P.$on_end(function() {
      return handler(null);
    }));
    return null;
  };

  this.$clean_stops_record = function() {
    return $((function(_this) {
      return function(record, handler) {
        delete record['stop_desc'];
        delete record['zone_id'];
        delete record['stop_url'];
        delete record['location_type'];
        delete record['parent_station'];
        return handler(null, record);
      };
    })(this));
  };

  this.main = function(registry, handler) {
    var gtfs_type, message, messages, method, method_name, no_method, no_source, ok_types, route, route_by_types, source_name, t0, tasks, _fn, _i, _j, _k, _len, _len1, _len2, _ref2, _ref3;
    t0 = 1 * new Date();
    for (source_name in datasource_infos) {
      route_by_types = datasource_infos[source_name];
      tasks = [];
      no_source = [];
      no_method = [];
      ok_types = [];
      _ref2 = options['data']['gtfs-types'];
      _fn = (function(_this) {
        return function(method_name, method, route) {
          return tasks.push(function(async_handler) {
            help("" + badge + "/" + method_name);
            return method(registry, route, function() {
              var P;
              P = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
              info("" + badge + "/" + method_name);
              return async_handler.apply(null, P);
            });
          });
        };
      })(this);
      for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
        gtfs_type = _ref2[_i];
        route = route_by_types[gtfs_type];
        if (route == null) {
          no_source.push("skipping " + source_name + "/" + gtfs_type + " (no source file)");
          continue;
        }
        help("found data source for " + source_name + "/" + gtfs_type);
        method_name = "read_" + gtfs_type;
        method = this[method_name];
        if (method == null) {
          no_method.push("no method to read GTFS data of type " + (rpr(gtfs_type)) + "; skipping");
          continue;
        }
        method = method.bind(this);
        ok_types.push(gtfs_type);
        _fn(method_name, method, route);
      }
      _ref3 = [no_source, no_method];
      for (_j = 0, _len1 = _ref3.length; _j < _len1; _j++) {
        messages = _ref3[_j];
        for (_k = 0, _len2 = messages.length; _k < _len2; _k++) {
          message = messages[_k];
          warn(message);
        }
      }
      info("reading data for " + ok_types.length + " type(s)");
      info("  (" + (ok_types.join(', ')) + ")");
    }
    ASYNC.series(tasks, (function(_this) {
      return function(error) {
        var t1;
        if (error != null) {
          throw error;
        }
        t1 = 1 * new Date();
        urge('dt:', (t1 - t0) / 1000);
        return handler(null, registry);
      };
    })(this));
    return null;
  };

  if (module.parent == null) {
    registry = REGISTRY.new_registry();
    this.main(registry, function(error, registry) {
      if (error != null) {
        throw error;
      }
      return info(registry);
    });
  }

}).call(this);
