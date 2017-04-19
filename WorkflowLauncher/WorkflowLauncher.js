var kafka = require('kafka-node')
var Producer = kafka.Producer
var Consumer = kafka.Consumer
var kafkaHost = "ubuntu";
var kafkaHostIP = "192.168.188.101";
var client = new kafka.Client(kafkaHost + ":2181/")
var localCacheAPI = require("./local-cache-api.js");
var localLoggerAPI = require("./local-logger-api.js");

var workflowEventsTopic = "workflowEvents";

// please create Kafka Topic before using this application in the VM running Kafka
// kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic workflowEvents

producer = new Producer(client);
KeyedMessage = kafka.KeyedMessage;
var APP_VERSION = "0.8"
var APP_NAME = "WorkflowLauncher"


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
  catch (e) {
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
  catch (e) {
    console.log("Exception in initializeKafkaConsumer" + e);
    console.log("Exception in initializeKafkaConsumer" + JSON.stringify(e));
    console.log("Try again in 5 seconds");
    setTimeout(initializeKafkaConsumer, 5000, attempt + 1);
  }
}//initializeKafkaConsumer

initializeKafkaConsumer(1);
initializeKafkaProducer(1);



console.log("Running " + APP_NAME + " version " + APP_VERSION);


// consume local workflowEvents from Kafka and produce RoutingSlip events for new workflow instances triggered by these events
function handleWorkflowEvent(eventMessage) {
  var event = JSON.parse(eventMessage.value);
  console.log("received message", eventMessage);
  if ("NewTweetEvent" == eventMessage.key) {
    console.log("A new tweet event has reached us. Time to act and publish a corresponding workflow event");

    message.payload = event.tweet;
    message.workflowConversationIdentifier = "OracleCodeTweetProcessor" + new Date().getTime();
    km = new KeyedMessage(message.workflowConversationIdentifier, JSON.stringify(message));

    payloads = [
      { topic: workflowEventsTopic, messages: [km], partition: 0 }
    ];

    producer.send(payloads, function (err, data) {
      console.log("Published event to topic " + workflowEventsTopic + " :" + JSON.stringify(data));
    });
    localLoggerAPI.log("Initialized new workflow OracleCodeTweetProcessor triggered by NewTweetEvent; stored workflowevent plus routing slip in cache under key " + message.workflowConversationIdentifier +" - (workflowConversationIdentifier:" + event.workflowConversationIdentifier + ")"
      , APP_NAME, "info");
    // PUT Workflow Event in Cache under workflow event identifier
    localCacheAPI.putInCache(message.workflowConversationIdentifier, message,
      function (result) {
        console.log("store workflowevent plus routing slip in cache under key " + message.workflowConversationIdentifier + ": " + JSON.stringify(result));
      });
  }//if 

}// handleWorkflowEvent




message =

  {
    "workflowType": "oracle-code-tweet-processor"
    , "workflowConversationIdentifier": "oracle-code-tweet-processor" + new Date().getTime()
    , "creationTimeStamp": new Date().getTime()
    , "creator": "WorkflowLauncher"
    , "actions":
    [{
      "id": "ValidateTweetAgainstFilters"
      , "type": "ValidateTweet"
      , "status": "new"  // new, inprogress, complete, failed
      , "result": "" // for example OK, 0, 42, true
      , "conditions": [] // a condition can be {"action":"<id of a step in the routingslip>", "status":"complete","result":"OK"}; note: the implicit condition for this step is that its own status = new   
    }
      , {
      "id": "EnrichTweetWithDetails"
      , "type": "EnrichTweet"
      , "status": "new"  // new, inprogress, complete, failed
      , "result": "" // for example OK, 0, 42, true
      , "conditions": [{ "action": "ValidateTweetAgainstFilters", "status": "complete", "result": "OK" }]
    }
      , {
      "id": "CaptureToTweetBoard"
      , "type": "TweetBoardCapture"
      , "status": "new"  // new, inprogress, complete, failed
      , "result": "" // for example OK, 0, 42, true
      , "conditions": [{ "action": "EnrichTweetWithDetails", "status": "complete", "result": "OK" }]
    }
    ]
    , "audit": [
      { "when": new Date().getTime(), "who": "WorkflowLauncher", "what": "creation", "comment": "initial creation as test case" }
    ]
    , "payload": {
      "text": "Fake 2 #oraclecode Tweet @StringSection"
      , "author": "lucasjellema"
      , "authorImageUrl": "http://pbs.twimg.com/profile_images/427673149144977408/7JoCiz-5_normal.png"
      , "createdTime": "April 17, 2017 at 01:39PM"
      , "tweetURL": "http://twitter.com/SaibotAirport/status/853935915714138112"
      , "firstLinkFromTweet": "https://t.co/cBZNgqKk0U"
    }
  };

