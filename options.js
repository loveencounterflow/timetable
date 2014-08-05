// Generated by CoffeeScript 1.7.1
(function() {
  var TRM, badge, data_home, data_info, error, njs_path, options, rpr, warn;

  njs_path = require('path');

  TRM = require('coffeenode-trm');

  rpr = TRM.rpr.bind(TRM);

  badge = 'timetable/options';

  warn = TRM.get_logger('warn', badge);

  try {
    data_info = require('timetable-data');
    data_home = (require('path')).dirname(require.resolve('timetable-data'));
  } catch (_error) {
    error = _error;
    warn("unable to `require 'timetable-data'`");
    help("please install data package with `npm install 'timetable-data'`");
    help("see " + (require('package.json'))['homepage'] + " for details");
  }

  warn(data_home);

  module.exports = options = {
    'parser': {
      'delimiter': ',',
      'skip_empty_lines': true
    },
    'stream-transform': {
      'parallel': 1
    },
    'data': {
      'types': require('./lib/gtfs-types'),
      'info': data_info,
      'home': data_home
    }
  };

}).call(this);
