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
  const Producer = kafka.Producer
  const client = new kafka.KafkaClient({
    kafkaHost: config.kafka_server,
    connectTimeout: 5000,
    sslOptions: {
      rejectUnauthorized: false
    }    
  })
  
  const producer = new Producer(client)
  const kafka_topic = config.kafka_topic
  
  let payloads = [
    {
      topic: kafka_topic,
      messages: config.kafka_topic
    }
  ];

  producer.on('ready', async function() {
    let push_status = producer.send(payloads, (err, data) => {
      if (err) {
        console.log(err)
        console.log('[kafka-producer -> '+kafka_topic+']: broker update failed')
      } else {
        console.log('[kafka-producer -> '+kafka_topic+']: broker update success')
      }
    })
  })

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    throw err;
  })
}
catch(e) {
  console.log(e);
}