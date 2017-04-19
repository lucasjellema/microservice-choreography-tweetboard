var kafka = require('kafka-node')
var request = require('request')
var Consumer = kafka.Consumer
var kafkaHost = "ubuntu";
var kafkaHostIP = "192.168.188.101";
var client = new kafka.Client(kafkaHost + ":2181/")

var logTopic = "logTopic";

// please create Kafka Topic before using this application in the VM running Kafka
// kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logTopic

var APP_VERSION = "0.8"
var APP_NAME = "Logger"


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
      handleLogEvent(message);
    });
    consumer.on('error', function (err) {
      console.log("error in creation of Kafka consumer " + JSON.stringify(err));
      console.log("Try again in 5 seconds");
      setTimeout(initializeKafkaConsumer, 5000, attempt + 1);
    });
    consumer.addTopics([
      { topic: logTopic, partition: 0, offset: 0 }
    ], () => console.log("topic added: " + logTopic));
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



console.log("Running " + APP_NAME + " version " + APP_VERSION);


var loggerRESTAPIURL = "https://artist-enricher-api-partnercloud17.apaas.us6.oraclecloud.com/logger-api";
//TODO go straight to WebLogic
// http://129.144.151.143/WebLogicLoggerService/resources/logger/log
/*
{
	"logLevel" : "info"
	,"module" : "nl.amis.soaring.logger.test"
	, "message" : "Avans Breda nl amis test moon sun watching hello from the other side"
	
}

http://129.144.151.143/
var bulkloggerRESTAPIURL = "http://129.144.151.143/SoaringTheWorldAtRestService/resources/logger/bulklog";
var loggerRESTAPIURL = "http://129.144.151.143/SoaringTheWorldAtRestService/resources/logger/log";

*/
//var directWebLoggerRESTAPIURL = "http://129.144.151.143/WebLogicLoggerService/resources/logger/log";
//var directWebLoggerRESTAPIURL = "http://129.144.151.143/SoaringTheWorldAtRestService/resources/logger/log";


// consume local workflowEvents from Kafka and produce RoutingSlip events for new workflow instances triggered by these events
function handleLogEvent(logMessage) {
  var logEntry = JSON.parse(logMessage.value);
  console.log("received message", logMessage);

    var args = {
        data: JSON.stringify(logEntry),
        headers: { "Content-Type": "application/json" }
    };

    var route_options = {};

    // Issue the POST  -- the callback will return the response to the user
    route_options.method = "POST";
    route_options.uri = loggerRESTAPIURL;
    console.log("Logger Target URL " + route_options.uri);

    route_options.body = args.data;
    route_options.headers = args.headers;
    console.log("*** Logger body for  message logging: " + args.data);



    request(route_options, function (error, rawResponse, body) {
        if (error) {
            console.log("*** Logger "+ JSON.stringify(error));
        } else {
            console.log("*** Logger "+rawResponse.statusCode);
            console.log("*** Logger "+"BODY:" + JSON.stringify(body));
        }//else
    }); //request

}// handleLogEvent

