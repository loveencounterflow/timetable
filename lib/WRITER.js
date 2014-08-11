// Generated by CoffeeScript 1.7.1
(function() {
  var $, ASYNC, COURSES, DEV, P, REGISTRY, TRM, alert, badge, debug, echo, help, info, log, options, rainbow, rpr, urge, warn, whisper;

  TRM = require('coffeenode-trm');

  rpr = TRM.rpr.bind(TRM);

  badge = 'TIMETABLE/READER';

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

  ASYNC = require('async');

  REGISTRY = require('./REGISTRY');

  COURSES = require('./COURSES');

  P = require('pipedreams');

  $ = P.$.bind(P);

  options = require('../options');

  DEV = options['mode'] === 'dev';

  this._distance_from_latlongs = function(latlong_a, latlong_b) {

    /* http://hashbang.co.nz/blog/2013/2/25/d3_js_geo_fun */

    /* http://www.geodatasource.com/developers/javascript */
    var R, lat1, lat2, lon1, lon2, radlat1, radlat2, radlon1, radlon2, theta;
    lat1 = latlong_a[0], lon1 = latlong_a[1];
    lat2 = latlong_b[0], lon2 = latlong_b[1];
    radlat1 = Math.PI * lat1 / 180;
    radlat2 = Math.PI * lat2 / 180;
    radlon1 = Math.PI * lon1 / 180;
    radlon2 = Math.PI * lon2 / 180;
    theta = (lon1 - lon2) * Math.PI / 180;
    R = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(theta);
    R = Math.acos(R);
    R = R * 180 / Math.PI;
    R = R * 60 * 1.1515;
    R = R * 1.609344;
    R = R * 1000;
    return R;
  };

  this.create_indexes = function(handler) {
    var constraint, constraints, identifier, identifiers, on_finish, tasks, _fn, _fn1, _i, _j, _len, _len1;
    tasks = [];
    constraints = ["CREATE CONSTRAINT ON (n:trip)     ASSERT n.id IS UNIQUE;", "CREATE CONSTRAINT ON (n:stop)     ASSERT n.id IS UNIQUE;", "CREATE CONSTRAINT ON (n:route)    ASSERT n.id IS UNIQUE;", "CREATE CONSTRAINT ON (n:stoptime) ASSERT n.id IS UNIQUE;"];
    identifiers = [':trip(`route-id`)', ':stop(`name`)', ':stoptime(`trip-id`)', ':stoptime(`stop-id`)', ':route(`name`)'];
    _fn = (function(_this) {
      return function(constraint) {
        var query;
        query = {
          query: constraint
        };
        return tasks.push(function(async_handler) {
          return N4J._request(query, function(error) {
            if (error != null) {
              return async_handler(error);
            }
            whisper("created constraint and index with " + constraint);
            return async_handler(null);
          });
        });
      };
    })(this);
    for (_i = 0, _len = constraints.length; _i < _len; _i++) {
      constraint = constraints[_i];
      _fn(constraint);
    }
    _fn1 = (function(_this) {
      return function(identifier) {
        var query;
        query = {
          query: "CREATE INDEX ON " + identifier + ";"
        };
        return tasks.push(function(async_handler) {
          return N4J._request(query, function(error) {
            if (error != null) {
              return async_handler(error);
            }
            whisper("created index on " + identifier);
            return async_handler(null);
          });
        });
      };
    })(this);
    for (_j = 0, _len1 = identifiers.length; _j < _len1; _j++) {
      identifier = identifiers[_j];
      _fn1(identifier);
    }
    on_finish = (function(_this) {
      return function(error) {
        if (error != null) {
          return handler(error);
        }
        return handler(null);
      };
    })(this);
    return ASYNC.series(tasks, on_finish);
  };

  this.update_distances = function(handler) {
    var query;
    query = {
      query: "MATCH (stop1:stop)-[r:distance]->(stop2:stop) RETURN DISTINCT stop1,stop2"
    };
    N4J._request(query, (function(_this) {
      return function(error, rows) {
        var distance_m, id1, id2, on_finish, stop1, stop2, tasks, _fn, _i, _len, _ref;
        if (error != null) {
          return handler(error);
        }
        tasks = [];
        _fn = function(query) {
          return tasks.push(function(async_handler) {
            return N4J._request(query, function(error) {
              if (error != null) {
                return async_handler(error);
              }
              return async_handler(null);
            });
          });
        };
        for (_i = 0, _len = rows.length; _i < _len; _i++) {
          _ref = rows[_i], stop1 = _ref[0], stop2 = _ref[1];
          id1 = stop1['id'];
          id2 = stop2['id'];
          distance_m = Math.floor(0.5 + _this._distance_from_latlongs(stop1['latlong'], stop2['latlong']));
          query = {
            query: "MATCH (stop1:stop {id: " + (N4J._escape(id1)) + "})-[r:distance]->(stop2:stop {id: " + (N4J._escape(id2)) + "})\nSET r.value = " + distance_m
          };
          _fn(query);
        }
        on_finish = function(error) {
          if (error != null) {
            return handler(error);
          }
          return handler(null);
        };
        return ASYNC.series(tasks, on_finish);
      };
    })(this));
    return null;
  };

  this.create_edges = function(handler) {
    var on_finish, source, sources, tasks, _fn, _i, _len;
    tasks = [];

    /* TAINT rewrite for halts, courses, reltrips */
    sources = ["MATCH (a:halt)\nMATCH (b:halt {`course-id`: a.`course-id`, `idx`: a.idx + 1})\nCREATE (a)-[:linked {`~label`: 'linked'}]->(b);"];
    _fn = (function(_this) {
      return function(source) {
        var query;
        query = {
          query: source
        };
        return tasks.push(function(async_handler) {
          return N4J._request(query, function(error) {
            if (error != null) {
              return async_handler(error);
            }
            whisper("created edges with\n" + source);
            return async_handler(null);
          });
        });
      };
    })(this);
    for (_i = 0, _len = sources.length; _i < _len; _i++) {
      source = sources[_i];
      _fn(source);
    }
    on_finish = (function(_this) {
      return function(error) {
        if (error != null) {
          return handler(error);
        }
        return handler(null);
      };
    })(this);
    ASYNC.series(tasks, on_finish);
    return null;
  };

  this.read_station_nodes = function(registry, handler) {
    var input;
    input = P.$read_values(registry['%gtfs']['stops']);
    return input.pipe(this.$normalize_station_name('name')).pipe(this.$add_n4j_system_properties('station')).pipe(this.$add_id()).pipe(this.$remove_gtfs_fields()).pipe(this.$register(registry)).pipe(P.$collect_sample(input, 1, function(_, sample) {
      return whisper('station', sample);
    })).on('end', (function(_this) {
      return function() {
        return handler(null);
      };
    })(this));
  };

  this.main = function(registry, handler) {
    var collection, node_type, t0, tasks, _i, _len, _ref;
    t0 = 1 * new Date();
    tasks = [];
    _ref = options['data']['node-types'];
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      node_type = _ref[_i];
      collection = registry[node_type];
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

}).call(this);