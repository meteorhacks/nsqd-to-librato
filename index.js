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

// catch error events
librato.on('error', function (e) {
  console.error(e);
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
  };

  if(prevTopics[name]) {
    var prev = prevTopics[name];
    var stats = {
      'ingress': curr.count - prev.count,
      'egress': curr.count - prev.count - (curr.depth - prev.depth),
    };

    for(var key in stats) {
      var field = [prefix, name, key].join('-')
      librato.increment(field, stats[key]);
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
  };

  if(prevChannels[topicName] && prevChannels[topicName][name]) {
    var prev = prevChannels[topicName][name];
    var stats = {
      'ingress': curr.count - prev.count,
      'egress': curr.count - prev.count - (curr.depth - prev.depth),
    };

    for(var key in stats) {
      var field = [prefix, topicName, name, key].join('-')
      librato.increment(field, stats[key]);
    }
  }

  if(!prevChannels[topicName]) {
    prevChannels[topicName] = {};
  }

  prevChannels[topicName][name] = curr
}
