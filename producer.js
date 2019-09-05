const kafka = require('kafka-node')
const bp = require('body-parser')
var emoji = require('node-emoji')
var location = require('random-location')


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

// set the random location code
const locatioAws = {
  latitude: 47.6181527,
  longitude: -122.3425197
}
const locationRadius = 70000 // meters

console.log(config)

// starts the client connection
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
  ]

  // declare empty randomPoint
  var randomPoint

  const produce = () => {
    // create the message
    randomPoint = location.randomCircumferencePoint(locatioAws, locationRadius)
    let message = {
      timestamp: new Date().toISOString(),
      latitude: randomPoint.latitude,
      longitude: randomPoint.longitude,
      temperature: Math.floor(Math.random() * 12)
    }
    // log the message
    console.log('> '  +  JSON.stringify(message))
    let push_status = producer.send(payloads, (err, data) => {
      if (err) {
        console.log('Operation failed: ')
        console.log(err)
      } else {
        console.log('Operation succesful')
        console.log(data)
      }
    })
  }
  
  producer.on('ready', async function() {
    setInterval(produce, 100)
  })
    

  producer.on('error', function(err) {
    console.log('Operation failed: ')
    console.log(err)
    throw err
  })
}
catch(e) {
  console.log(e);
}