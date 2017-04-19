var request = require('request');
var fs = require('fs');
var logger = require("./logger.js");

// not available locally, only on ACCS 
var settings = require("./proxy-settings.js");



var eventHubAPI = module.exports;
var apiURL = "/eventhub-api";
var moduleName = "accs.eventhubAPI";


var eventhubURL = "https://129.144.152.204:1080/restproxy";
var topicName = "partnercloud17-SoaringEventBus";
var groupName = "soaring-events-consumer-group";
var consumerName = "soaring-events-consumer";


eventHubAPI.registerListeners =
    function (app) {

        app.get(apiURL + '/about', function (req, res) {
            console.log('event Hub: about');
            eventHubAPI.handleAbout(req, res);
        });
        app.post(apiURL + '/messages', function (req, res) {
            console.log('EventHub-API POST - now show params');
            console.log('EventHub-API POST params ' + JSON.stringify(req.params));
            console.log('body in request' + JSON.stringify(req.body));
            eventHubAPI.postMessages(req, res, req.body);
        });//post messages


        app.post(apiURL + '/create-consumer-group/:consumerGroupName', function (req, res) {
            var consumerGroupName = req.params['consumerGroupName'];	// to retrieve value of query parameter called artist (?artist=someValue&otherParam=X)
            eventHubAPI.createConsumerGroup(consumerGroupName, function(response){
               res.json(response).end();
            });
        });//post consumer group

        app.post(apiURL + '/create-consumer-group', function (req, res) {
            var consumerGroupName = groupName;
            eventHubAPI.createConsumerGroup(consumerGroupName, function(response){
               res.json(response).end();
            });
        });//post consumer group

        app.get(apiURL + '/messages/:consumerGroupName', function (req, res) {
            var consumerGroupName = req.params['consumerGroupName'];	// to retrieve value of query parameter called artist (?artist=someValue&otherParam=X)
            console.log('EventHub-API GET  - get messages for consumer group ' + consumerGroupName);
            eventHubAPI.getMessages(req, res, consumerGroupName);
        });//get messages for consumer group


        app.get(apiURL + '/messages', function (req, res) {
            console.log('EventHub-API GET  - get messages');
            eventHubAPI.getMessages(req, res, groupName);
        });//get messages

    }//registerListeners

console.log("EventHub API (version " + settings.APP_VERSION + ") initialized at " + apiURL + " running against EventHub Service URL " + eventhubURL);



eventHubAPI.handleAbout = function (req, res) {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.write("EventHub-API - About - Version " + settings.APP_VERSION + ". ");
    res.write("Supported URLs:");
    res.write(apiURL + "/messages (GET)");
    res.write(apiURL + "/messages/:consumerGroupName (GET)");
    res.write(apiURL + "/createConsumerGroup (POST)");
    res.write(apiURL + "/createConsumerGroup/:consumerGroupName (POST)");
    res.write("Supported URLs:");
    res.write("incoming headers" + JSON.stringify(req.headers));
    res.end();
}//handleAbout

eventHubAPI.postMessages = function (req, res, messages) {
    postMessagesToEventHub(messages, function (response) {
        res.json(response).end();
    })
}// postMessages

eventHubAPI.postMessagesToEventHub = function (messages, callback) {
    var route_options = {
        /*key: fs.readFileSync('ssl/server1.key'),*/
        cert: fs.readFileSync('./event-hub.pem'),
        requestCert: true,
        rejectUnauthorized: false,
    };

    // Issue the POST  -- the callback will return the response to the user
    route_options.method = "POST";
    route_options.uri = eventhubURL.concat('/topics/').concat(topicName);

    console.log("Target URL " + route_options.uri);
    var args = {
        data: {},
        headers: {
            "Content-Type": "application/vnd.kafka.json.v1+json"
            , "Authorization": "Basic YWRtaW46T29vdzIwMTY="
        }
    };
    args.data = messages;
    route_options.body = JSON.stringify(args.data);
    route_options.headers = args.headers;
    request(route_options, function (error, rawResponse, body) {
        if (error) {
            console.log("Error in call " + JSON.stringify(error));
        } else {
            console.log(rawResponse.statusCode);
            console.log("BODY:" + JSON.stringify(body));
            // Proper response is 204, no content.
            var responseBody = {};
            responseBody['status'] = 'POST  returned '.concat(rawResponse.statusCode.toString());
            if (callback) {
                callback(responseBody);
            }
        }//else
    });//request

}// postMessages



eventHubAPI.getMessages = function (req, res, consumerGroupName) {
    eventHubAPI.getMessagesFromConsumerGroup(consumerGroupName, function (response) {
        // Send the response
        res.json(response).end();
    });

}//getMessages

eventHubAPI.getMessagesFromConsumerGroup = function (consumerGroupName, callback) {
    var route_options = {
        /*key: fs.readFileSync('ssl/server1.key'),*/
        cert: fs.readFileSync('./event-hub.pem'),
        requestCert: true,
        rejectUnauthorized: false,
    };
    // Issue the POST  -- the callback will return the response to the user
    route_options.method = "GET";
    ///restproxy/consumers/{groupName}/instances/{consumerInstanceId}/topics/{topicName}
    //consumers/soaring-consumer-group/instances/soaring-event-consumer
    //https://129.144.152.204:1080/restproxy/consumers/soaring-consumer-group/instances/soaring-consumer/partnercloud17-SoaringEventBus
    //route_options.uri = eventhubURL.concat('/consumers/').concat(groupName).concat("/instances/").concat(consumerName).concat("/").concat(topicName);
    route_options.uri = eventhubURL.concat('/consumers/').concat(consumerGroupName).concat("/instances/").concat(consumerName).concat("/topics/").concat(topicName);

    console.log("Target URL " + route_options.uri);
    var args = {
        headers: {
            "Content-Type": "application/vnd.kafka.json.v1+json"
            , "Accept": "application/vnd.kafka.json.v1+json"
            , "Authorization": "Basic YWRtaW46T29vdzIwMTY="
        }
    };

    route_options.headers = args.headers;



    request(route_options, function (error, rawResponse, body) {
        if (error) {
            console.log("Error in call " + JSON.stringify(error));
        } else {
            console.log(rawResponse.statusCode);
            console.log("BODY:" + JSON.stringify(body));
            //"{\"instance_id\":\"soaring-consumer\",\"base_uri\":\"http://localhost:8082/consumers/soaring-consumer-group/instances/soaring-consumer\"}"

            // Proper response is 204, no content.
            var responseBody = {};
            responseBody['status'] = 'GET  returned '.concat(rawResponse.statusCode.toString());
            responseBody['statusCode'] = rawResponse.statusCode;
            responseBody['body'] = body;
            callback(responseBody);
        }//else
    });//request

}//getMessagesFromConsumerGroup


eventHubAPI.createConsumerGroup = function ( consumerGroupName, callback) {
    var route_options = {
        /*key: fs.readFileSync('ssl/server1.key'),*/
        cert: fs.readFileSync('./event-hub.pem'),
        requestCert: true,
        rejectUnauthorized: false,
    };
    // Issue the POST  -- the callback will return the response to the user
    route_options.method = "POST";
    route_options.uri = eventhubURL.concat('/consumers/').concat(consumerGroupName);

    console.log("Target URL " + route_options.uri);
    var args = {
        data: {
            "name": consumerName,
            "format": "json",
            "auto.offset.reset": "smallest",
            "auto.commit.enable": "false"
        },
        headers: {
            "Content-Type": "application/vnd.kafka.json.v1+json"
            , "Authorization": "Basic YWRtaW46T29vdzIwMTY="
        }
    };

    route_options.body = JSON.stringify(args.data);
    route_options.headers = args.headers;

    request(route_options, function (error, rawResponse, body) {
        if (error) {
            console.log("Error in call " + JSON.stringify(error));
        } else {
            console.log(rawResponse.statusCode);
            console.log("BODY:" + JSON.stringify(body));
            //"{\"instance_id\":\"soaring-consumer\",\"base_uri\":\"http://localhost:8082/consumers/soaring-consumer-group/instances/soaring-consumer\"}"

            // Proper response is 204, no content.
            var responseBody = {};
            responseBody['status'] = 'POST  returned '.concat(rawResponse.statusCode.toString());
            responseBody['body'] = body;
            callback(responseBody);
        }//else
    });//request

}//createConsumerGroup

