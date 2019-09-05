const kafka = require('kafka-node')
const bp = require('body-parser')
var emoji = require('node-emoji')


// parse and vaildates intiial arguments
var args = require('minimist')(process.argv.slice(2))

const usage = () => {
  console.log("> node consumer $CLUSTER $TOPIC")
  console.log("> node consumer my.server.com myTopic")
}

if (!args._[0]) {
  console.error(emoji.get('fire') + ' Enter a server name like...')
  usage()
  return
}
if (!args._[1]) {
  console.error(emoji.get('fire') + ' Enter a topic name like...')
  usage()
  return
}

// set the config
const config = {
  kafka_server: args._[0],
  kafka_topic: args._[1]
}

var Consumer = kafka.Consumer,
    client = new kafka.KafkaClient({
      kafkaHost: config.kafka_server,
      connectTimeout: 5000,
      sslOptions: {
        rejectUnauthorized: false
      }
    }),
    consumer = new Consumer(client,
        [{ topic: config.kafka_topic, offset: 0}],
        {
            autoCommit: false
        }
    );

consumer.on('message', (message) => {
  console.log(message)
});

consumer.on('error', (err) => {
  console.log('Error:', err)
})

consumer.on('offsetOutOfRange', (err) => {
  console.log('offsetOutOfRange:', err)
})    