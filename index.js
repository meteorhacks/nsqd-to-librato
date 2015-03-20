var env = require('envalid');
var request = require('request');
var librato = require('librato-node');

env.validate(process.env, {
  PREFIX: {required: true},
  INTERVAL: {required: true, parse: env.toNumber},
  ADDRESS: {required: true},
  LIBRATO_EMAIL: {required: true},
  LIBRATO_TOKEN: {required: true},
});

var prefix = env.get('PREFIX');
var interval = env.get('INTERVAL');
var address = env.get('ADDRESS');
var prevTopics = {};
var prevChannels = {};

librato.configure({
  email: env.get('LIBRATO_EMAIL'),
  token: env.get('LIBRATO_TOKEN'),
});

// start librato
librato.start();

// stop librato on SIGINT
process.once('SIGINT', function() {
  librato.stop();
});

// start sending metrics
sendMetrics();

function sendMetrics () {
  request.get(address + '/stats?format=json', function (err, res) {
    if(err || !res || !res.body) {
      console.error(err || 'missing content');
    } else {
      processRequest(res.body);
    }

    setTimeout(sendMetrics, interval);
  });
}

function processRequest(body) {
  try {
    var data = JSON.parse(body);
    data.data.topics.forEach(processTopic);
  } catch(e) {
    console.error(e);
  }
}

function processTopic(topic) {
  var name = topic.topic_name;
  var curr = {
    count: topic.message_count,
    depth: topic.depth,
    _time: Date.now(),
  };

  if(prevTopics[name]) {
    var prev = prevTopics[name];
    var duration = (curr._time - prev._time)/1000;
    var stats = {
      'ingress': (curr.count - prev.count)/duration,
      'egress': (curr.count - prev.count - (curr.depth - prev.depth))/duration,
    };

    for(var key in stats) {
      var field = [prefix, name, key].join('-')
      librato.measure(field, stats[key]);
    }
  }

  prevTopics[name] = curr;

  // process channels
  topic.channels.forEach(function (channel) {
    processChannel(name, channel);
  });
}

function processChannel (topicName, channel) {
  var name = channel.channel_name;
  var curr = {
    count: channel.message_count,
    depth: channel.depth,
    flight: channel.in_flight_count,
    _time: Date.now(),
  };

  if(prevChannels[topicName] && prevChannels[topicName][name]) {
    var prev = prevChannels[topicName][name];
    var duration = (curr._time - prev._time)/1000;
    var stats = {
      'ingress': (curr.count - prev.count)/duration,
      'egress': (curr.count - prev.count - (curr.depth - prev.depth))/duration,
      'inflight': curr.flight - prev.flight,
    };

    for(var key in stats) {
      var field = [prefix, topicName, name, key].join('-')
      librato.measure(field, stats[key]);
    }
  }

  if(!prevChannels[topicName]) {
    prevChannels[topicName] = {};
  }

  prevChannels[topicName][name] = curr
}
