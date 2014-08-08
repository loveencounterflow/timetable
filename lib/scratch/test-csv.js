// Generated by CoffeeScript 1.7.1
(function() {
  var $, ES, S, TEXT, TRM, TYPES, alert, badge, create_readstream, debug, echo, help, info, log, njs_fs, rainbow, route, rpr, urge, warn, whisper;

  njs_fs = require('fs');

  TYPES = require('coffeenode-types');

  TEXT = require('coffeenode-text');

  TRM = require('coffeenode-trm');

  rpr = TRM.rpr.bind(TRM);

  badge = 'TIMETABLE/read-gtfs-data';

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

  create_readstream = require('../create-readstream');


  /* http://c2fo.github.io/fast-csv/index.html, https://github.com/C2FO/fast-csv */

  S = require('string');

  ES = require('event-stream');

  $ = ES.map.bind(ES);

  this.$count = function(input_stream, title) {
    var count;
    count = 0;
    input_stream.on('end', function() {
      return info((title != null ? title : 'Count') + ':', count);
    });
    return $((function(_this) {
      return function(record, handler) {
        count += 1;
        return handler(null, record);
      };
    })(this));
  };

  this.$parse_csv = function() {
    var field_names;
    field_names = null;
    return $((function(_this) {
      return function(record, handler) {
        var idx, value, values, _i, _len;
        values = (S(record)).parseCSV(',', '"', '\\');
        if (field_names === null) {
          field_names = values;
          return handler();
        }
        record = {};
        for (idx = _i = 0, _len = values.length; _i < _len; idx = ++_i) {
          value = values[idx];
          record[field_names[idx]] = value;
        }
        return handler(null, record);
      };
    })(this));
  };

  this.$skip_empty = function() {
    return $((function(_this) {
      return function(record, handler) {
        if (record.length === 0) {
          return handler();
        }
        return handler(null, record);
      };
    })(this));
  };

  this.$show = function() {
    return $((function(_this) {
      return function(record, handler) {
        urge(rpr(record));
        return handler(null, record);
      };
    })(this));
  };

  this.read_trips = function(route, handler) {
    var input;
    input = njs_fs.createReadStream(route);
    input.on('end', function() {
      log('ok: trips');
      return handler(null);
    });
    input.setMaxListeners(100);
    input.pipe(ES.split()).pipe(this.$skip_empty()).pipe(this.$parse_csv()).pipe(this.$count(input, 'trips A')).pipe(this.$show());
    return null;
  };

  if (!module.parent) {
    route = '/Volumes/Storage/cnd/node_modules/timetable-data/germany-berlin-2014/agency.txt';
    this.read_trips(route, function(error) {
      if (error != null) {
        throw error;
      }
      return log('ok');
    });
  }

}).call(this);