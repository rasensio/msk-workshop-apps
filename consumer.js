const kafka = require('kafka-node')
const bp = require('body-parser')
var emoji = require('node-emoji')


// parse and vaildates intiial arguments
var args = require('minimist')(process.argv.slice(2))
console.dir(args)

const usage = () => {
  console.log("> npm start SERVER TOPIC")
  console.log("> npm start my.server.com myTopic")
}

if (!args[0]) {
  console.error(emoji.get('fire') + ' Enter a server name like...')
  usage()
  return
}
if (!args[1]) {
  console.error(emoji.get('fire') + ' Enter a topic name like...')
  usage()
  return
}

// set the config
const config = {
  kafka_server: args[0],
  kafka_topic: args[1]
}

try {
  const Consumer = kafka.HighLevelConsumer;
  const client = new kafka.Client(config.kafka_server);
  let consumer = new Consumer(
    client,
    [{ topic: config.kafka_topic, partition: 0 }],
    {
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      fromOffset: false
    }
  );
  consumer.on('message', async function(message) {
    console.log('here');
    console.log(
      'kafka-> ',
      message.value
    );
  })
  consumer.on('error', function(err) {
    console.log('error', err);
  });
}
catch(e) {
  console.log(e);
}
