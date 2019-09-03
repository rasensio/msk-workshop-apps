const kafka = require('kafka-node')
const bp = require('body-parser')
var emoji = require('node-emoji')


// parse and vaildates intiial arguments
var args = require('minimist')(process.argv.slice(2))
console.dir(args._[0])

const usage = () => {
  console.log("> npm start SERVER TOPIC")
  console.log("> npm start my.server.com myTopic")
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

console.log(config)

try {
  const client = new kafka.KafkaClient({
    kafkaHost: config.kafka_server,
    connectTimeout: 5000,
    sslOptions: {
      rejectUnauthorized: false
    }    
  })
  
  const topics = [{
    topic: config.kafka_topic,
    partitions: 2,
    replicationFactor: 3
  }]
  
  producer.on('ready', async function() {

    client.createTopics(topics, (error, result) => {
      if (err) {
        console.log(err)
        console.log('[kafka-topic -> '+kafka_topic+']: broker update failed')
      } else {
        console.log('[kafka-topic -> '+kafka_topic+']: broker update success')
      }
    })
  })

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-topic -> '+kafka_topic+']: connection errored');
    throw err;
  })
}
catch(e) {
  console.log(e);
}