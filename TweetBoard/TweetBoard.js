var kafka = require('kafka-node')
var http = require('http'),
  request = require('request'),
  express = require('express'),
  bodyParser = require('body-parser');
var localCacheAPI = require("./local-cache-api.js");
var localLoggerAPI = require("./local-logger-api.js");

var kafkaHost = "ubuntu";
var kafkaHostIP = "192.168.188.101";
var Consumer = kafka.Consumer
var Producer = kafka.Producer
KeyedMessage = kafka.KeyedMessage;

var workflowEventsTopic = "workflowEvents";
var client;
var consumer;
var PORT = process.env.PORT || 8096;
var APP_VERSION = "0.8"
var APP_NAME = "TweetBoard"

var TweetBoardCaptureActionType = "TweetBoardCapture";
var tweetBoardDocumentKey = "tweetboard-document";


console.log("Running " + APP_NAME + "version " + APP_VERSION);


var app = express();
var server = http.createServer(app);
server.listen(PORT, function () {
  console.log('Microservice' + APP_NAME + ' running, Express is listening... at ' + PORT + " for /ping, /about and /tweetBoard API calls");
});

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json({ type: '*/*' }));
app.get('/about', function (req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.write("About TweetBoard API, Version " + APP_VERSION);
  res.write("Supported URLs:");
  res.write("/ping (GET)\n;");
  res.write("/tweetBoard (GET)");
  res.write("NodeJS runtime version " + process.version);
  res.write("incoming headers" + JSON.stringify(req.headers));
  res.end();
});

app.get('/ping', function (req, res) {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.write("Reply from " + APP_NAME);
  res.write("incoming headers" + JSON.stringify(req.headers));
  res.end();
});

app.get('/tweetBoard', function (req, res) {
  // Get the key and value
  console.log('TweetBoard - get document tweetboard from cache');
  var responseBody = { "result": "to be fetched from cache", "motivation": "to be provided" };
  localCacheAPI.getFromCache(tweetBoardDocumentKey, function (document) {
    console.log("tweetboard document retrieved from cache");
    // Send the response
    res.setHeader('Content-Type', 'application/json');
    res.send(document);
  });
});


function initializeKafkaProducer(attempt) {
  try {
    console.log("Try to initialize Kafka Client and Producer, attempt " + attempt);
    var client2 = new kafka.Client(kafkaHost + ":2181/")
    console.log("created client2");
    producer = new Producer(client);
    console.log("submitted async producer creation request");
    producer.on('ready', function () {
      console.log("Producer is ready in " + APP_NAME);
    });
    producer.on('error', function (err) {
      console.log("failed to create the client or the producer " + JSON.stringify(err));
    })
  }
  catch (e) {
    console.log("Exception in initializeKafkaConsumer" + e);
    console.log("Exception in initializeKafkaConsumer" + JSON.stringify(e));
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

function handleWorkflowEvent(eventMessage) {
  var event = JSON.parse(eventMessage.value);
  console.log("received message", eventMessage);
  console.log("received message object", JSON.stringify(eventMessage));
  console.log("actual event: " + JSON.stringify(event));

  // event we expect is of type workflowEvents
  // we should do something with this event if it contains an action (actions[].type='ValidateTweet' where status ="new" and conditions are satisfied)

  if (event.actions) {
    var acted = false;
    localCacheAPI.getFromCache(tweetBoardDocumentKey, function (doc) {
      console.log("tweetboard document retrieved from cache");

      for (i = 0; i < event.actions.length; i++) {
        var action = event.actions[i];
        // find action of type ValidateTweet
        if (TweetBoardCaptureActionType == action.type) {
          // check conditions
          if ("new" == action.status
            && conditionsSatisfied(action, event.actions)) {
            var workflowDocument;
            // add workflow tweet payload to the tweetboard document
            // reverse messages to have last/most recent one first
            doc.tweets.reverse();
            doc.tweets.push(event.payload);
            // most recent ones at the top
            doc.tweets.reverse();
            // retain no more than 25 entries
            doc.tweets = doc.tweets.slice(0, 25);

            localLoggerAPI.log("Added Tweet to TweetBoard - (workflowConversationIdentifier:" + event.workflowConversationIdentifier + ")"
              , APP_NAME, "info");
            // update action in event
            action.status = 'complete';
            // add audit line
            event.audit.push(
              { "when": new Date().getTime(), "who": "TweetBoard", "what": "update", "comment": "Tweet Board Capture done" }
            );
            acted = true;
          }
        }// if TweetBoardCaptureActionType
        else {
          // localLoggerAPI.log("Conditions not (yet) met for action " + action.id + " - (workflowConversationIdentifier:" + event.workflowConversationIdentifier + ")"
          //   , APP_NAME, "debug");

        }
        // if any action performed, then republish workflow event and store routingslip in cache
      }//for
      if (acted) {
        // PUT Workflow Document back  in Cache under workflow event identifier
        localCacheAPI.putInCache(event.workflowConversationIdentifier, event,
          function (result) {
            console.log("store workflowevent plus routing slip in cache under key " + event.workflowConversationIdentifier + ": " + JSON.stringify(result));
          });

        event.updateTimeStamp = new Date().getTime();
        event.lastUpdater = APP_NAME;
        // publish event

        km = new KeyedMessage('OracleCodeTwitterWorkflow' + event.updateTimeStamp, JSON.stringify(event));
        payloads = [
          { topic: workflowEventsTopic, messages: [km], partition: 0 }
        ];
        producer.send(payloads, function (err, data) {
          console.log("Published workflow event to topic " + workflowEventsTopic + " :" + JSON.stringify(data));
        });

        // put tweetboard document in the cache
        localCacheAPI.putInCache(tweetBoardDocumentKey, doc,
          function (result) {
            console.log("stored tweetboard document cache under key " + tweetBoardDocumentKey + ": " + JSON.stringify(result));
          });

      }// acted
    });//getFromCache
  }// if actions
}// handleWorkflowEvent

function conditionsSatisfied(action, actions) {
  var satisfied = true;
  // verify if conditions in action are methodName(params) {
  //   example action: {
  //   "id": "CaptureToTweetBoard"
  // , "type": "TweetBoardCapture"
  // , "status": "new"  // new, inprogress, complete, failed
  // , "result": "" // for example OK, 0, 42, true
  // , "conditions": [{ "action": "EnrichTweetWithDetails", "status": "complete", "result": "OK" }]
  for (i = 0; i < action.conditions.length; i++) {
    var condition = action.conditions[i];
    if (!actionWithIdHasStatusAndResult(actions, condition.action, condition.status, condition.result)) {
      satisfied = false;
      break;
    }
  }//for
  return satisfied;
}//conditionsSatisfied

function actionWithIdHasStatusAndResult(actions, id, status, result) {
  for (i = 0; i < actions.length; i++) {
    if (actions[i].id == id && actions[i].status == status && actions[i].result == result)
      return true;
  }//for
  return false;
}//actionWithIdHasStatusAndResult

