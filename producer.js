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

  const produce = () => {
    // create the message with a random deviceId between 1 and 10
    let message = {
      timestamp: new Date().toISOString(),
      deviceId: Math.floor(Math.random() * 10) + 1,
      temperature: Math.floor(Math.random() * 12)
    }
    message = JSON.stringify(message)
    // log the message
    console.log('> ' + message)
    let payload = {
      topic: config.kafka_topic,
      messages: [message]
    }

    let push_status = producer.send(payload, (err, data) => {
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