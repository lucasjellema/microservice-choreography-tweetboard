var kafka = require('kafka-node')
var http = require('http'),
  request = require('request'),
  express = require('express'),
  bodyParser = require('body-parser');
var eventhubAPI = require("./eventhub-api.js");
var localLoggerAPI = require("./local-logger-api.js");

var kafkaHost = "ubuntu";
var kafkaHostIP = "192.168.188.101";
var Consumer = kafka.Consumer
var Producer = kafka.Producer
var eventHubConsumerGroupName = "onPremisesEventBridge";
KeyedMessage = kafka.KeyedMessage;


var refreshInterval = 10; //seconds cycle in checking event hub for events

var workflowEventsTopic = "workflowEvents";
var client;
var consumer;
var PORT = process.env.PORT || 8095;
var APP_VERSION = "0.8"
var APP_NAME = "EventBridge"

console.log("Running TweetValidator version " + APP_VERSION);


var app = express();
var server = http.createServer(app);
server.listen(PORT, function () {
  console.log('Microservice is ' + APP_NAME + ' running, Express is listening... at ' + PORT + " for /ping, /about and /events (POST). The microservice also monitors Kafka Topic " + workflowEventsTopic);
});

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json({ type: '*/*' }));
app.get('/about', function (req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.write("About EventBridge API, Version " + APP_VERSION);
  res.write("Supported URLs:");
  res.write("/ping (GET)\n;");
  res.write("/events (POST)");
  res.write("NodeJS runtime version " + process.version);
  res.write("incoming headers" + JSON.stringify(req.headers));
  res.end();
});

app.get('/ping', function (req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.write("Reply");
  res.write("incoming headers" + JSON.stringify(req.headers));
  res.end();
});

app.post('/events', function (req, res) {
  // Get the key and value
  console.log('EventBridge Publish Events to local Kafka Topic');
  console.log('body in request' + JSON.stringify(req.body));
  console.log("content type " + req.headers['content-type']);
  var events = req.body;
  var responseBody = { "nothing yet": "" };
  // Send the response
  res.setHeader('Content-Type', 'application/json');
  res.send(responseBody);

});

function initializeKafkaProducer(attempt) {
  try {
    console.log("Try to initialize Kafka Client and Producer, attempt " + attempt);
    var client2 = new kafka.Client(kafkaHost + ":2181/")
    console.log("created client2");
    producer = new Producer(client);
    console.log("submitted async producer creation request");
    producer.on('ready', function () {
      console.log("Producer is ready");
    });
    producer.on('error', function (err) {
      console.log("failed to create the client or the producer " + JSON.stringify(err));
    })
  }
  catch (err) {
    console.log("Exception in initializeKafkaProducer" + e);
    console.log("Exception in initializeKafkaProducer" + JSON.stringify(e));
    console.log("Try again in 5 seconds");
    setTimeout(initializeKafkaProducer, 5000, ++attempt);
  }
}//initializeKafkaProducer

function initializeKafkaConsumer(attempt) {
  try {
    console.log("Try to initialize Kafka Client and Consumer, attempt " + attempt);
    client = new kafka.Client(kafkaHost + ":2181/")
    console.log("created client");
    consumer = new Consumer(
      client,
      [],
      { fromOffset: true }
    );
    console.log("Kafka Client and Consumer initialized " + consumer);
    // register the handler for any messages received by the consumer on any topic it is listening to. 
    consumer.on('message', function (message) {
      console.log("event received");
      handleWorkflowEvent(message);
    });
    consumer.on('error', function (err) {
      console.log("error in creation of Kafka consumer " + JSON.stringify(err));
      console.log("Try again in 5 seconds");
      setTimeout(initializeKafkaConsumer, 5000, attempt + 1);
    });
    consumer.addTopics([
      { topic: workflowEventsTopic, partition: 0, offset: 0 }
    ], () => console.log("topic added: " + workflowEventsTopic));
    console.log("Kafka Consumer - added message handler and added topic");
  }
  catch (err) {
    console.log("Exception in initializeKafkaConsumer" + e);
    console.log("Exception in initializeKafkaConsumer" + JSON.stringify(e));
    console.log("Try again in 5 seconds");
    setTimeout(initializeKafkaConsumer, 5000, attempt + 1);
  }
}//initializeKafkaConsumer

initializeKafkaConsumer(1);
initializeKafkaProducer(1);


// consume local workflowEvents from Kafka and send onwards to EventHub
function handleWorkflowEvent(eventMessage) {
  // any workflow event has to be forwarded to the Eventbridge in the cloud
  var event = JSON.parse(eventMessage.value);
  if (!event.eventBridgeMarker) {
    event.eventBridgeMarker = 'on-premises';
    console.log("received message", eventMessage);
    console.log("received message object", JSON.stringify(eventMessage));
    console.log("about to forward to EventBridge in the cloud : " + JSON.stringify(event));
  } else {
    // marker already in event; this event should not be forwarded
    console.log("EventBridgeMarker already in this event so it is not forwarded to the cloud event bridge. " + JSON.stringify(event));
  }
}// handleWorkflowEvent

//poll the EventHub every 10 seconds for new events to locally Publish to Kafka
initHeartbeat = function (interval) {
  setInterval(function () {
    checkEventHub();
  }
    , interval ? interval * 1000 : refreshInterval
  ); // setInterval 3000 is 3 secs
}//initHeartbeat


initHeartbeat(refreshInterval)

function checkEventHub() {
  //console.log('check likes');
  eventhubAPI.createConsumerGroup(eventHubConsumerGroupName, function (response) {
    console.log("Local EventBridge - after create consumer group " + response);
    eventhubAPI.getMessagesFromConsumerGroup(eventHubConsumerGroupName, function (response) {
      var messages = JSON.parse(response.body);
      if (messages) {
        console.log("*** EventBridge : received messages from Event Hub");
        processEvents(messages);
      } else {
        console.log("*** EventBridge: no messages available from Event Hub");
      }

    }); // eventHubAPI get messages
  }); //eventhubAPI create consumer group


}//checkLogs

var eventsOffset = 0;


function processEvents(messages, offset) {
  for (index = 0; index < messages.length; ++index) {
    console.log("index = " + index);
    console.log("msg  = " + messages[index]);

    msg = messages[index];
    // only process messages beyond the current offset
    if (msg.offset > eventsOffset && msg.key != 'log') {
      // TODO which keys do we support - one will be NewTweetEvent from EventBridge in the cloud
      var key = msg.key;
      var value = msg.value;
      // add event bridge marker
      value.eventBridgeMarker = 'on-premises';
      //now publish to Kafka producer
      var km = new KeyedMessage(key, JSON.stringify(value));
      var payloads = [
        { topic: workflowEventsTopic, messages: [km], partition: 0 }
      ];
      localLoggerAPI.log("Consumed event from EventHub and Forwarded to local Kafka Topic " + workflowEventsTopic + JSON.stringify(value)
        , APP_NAME, "debug");
      producer.send(payloads, function (err, data) {
        console.log("Published event to topic " + workflowEventsTopic + " :" + JSON.stringify(data));
      });
    }//if
  }//for
  if (messages && messages.length > 0) {
    eventsOffset = messages[messages.length - 1].offset;
  }
}//processEvents

