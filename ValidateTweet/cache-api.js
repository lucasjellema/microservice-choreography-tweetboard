var https = require('https'),
    http = require('http'),
    fs = require('fs'),
    url = require('url'),
    request = require('request'),
    qs = require('querystring'),
    bodyParser = require('body-parser')
    ;
var logger = require("./logger.js");

var mv = require('./multivalue.js');
// not available locally, only on ACCS 
var settings = require("./proxy-settings.js");

// Create a client that will "talk" to CCS
//var Client = require("node-rest-client").Client;

//var client = new Client();


var cacheAPI = module.exports;
var apiURL = "/cache-api";
var moduleName = "accs.cacheAPI";

var CCSHOST = process.env.CACHING_INTERNAL_CACHE_URL || "dummy_host";
//var CCSHOST = "/";
var baseCCSURL = 'http://' + CCSHOST + ':8080/ccs/';
// The name of our cache
var cacheName = 'soaringCache';


cacheAPI.registerListeners =
    function (app) {

        app.get(apiURL + '/about', function (req, res) {
            console.log('cache-api: about');
            handleAbout(req, res);
        });
        app.get(apiURL + '/:keyString', function (req, res) {
            console.log('Cache-API GET');
            console.log('Cache-API GET params ' + JSON.stringify(req.params));
            var keyString = req.params['keyString'];	// to retrieve value of query parameter called artist (?artist=someValue&otherParam=X)
            console.log("keystring " + keyString);
            logger.log("*** CacheAPI: request value from cache under key " + keyString, moduleName, logger.DEBUG);
            cacheAPI.getFromCache(keyString, function (response) {                    // Send the response
                res.json(response).end();
            });
        });// get

        app.put(apiURL + '/:keyString', function (req, res) {
            console.log('Cache-API PUT - now show params');
            console.log('Cache-API PUT params ' + JSON.stringify(req.params));
            console.log('body in request' + JSON.stringify(req.body));
            console.log("content type " + req.headers['content-type']);
            var keyString = req.params['keyString'];	// to retrieve value of query parameter called artist (?artist=someValue&otherParam=X)
            console.log("keystring " + keyString);
            logger.log("*** CacheAPI: stored value in cache for key " + keyString, moduleName, logger.INFO);
            var valString = JSON.stringify(req.body);
            console.log("value submitted in PUT to be stored in Cache" + valString);
            cacheAPI.putInCache(keyString, valString, function (response) {
                // Send the response
                res.json(response).end();
            }
            )
        });//put
        app.post(apiURL + '/:keyString', function (req, res) {
            // Get the key and value
            console.log('Cache-API POST - now show params');
            console.log('Cache-API POST params ' + JSON.stringify(req.params));
            console.log('body in request' + JSON.stringify(req.body));
            console.log("content type " + req.headers['content-type']);
            var keyString = req.params['keyString'];	// to retrieve value of query parameter called artist (?artist=someValue&otherParam=X)
            console.log("keystring " + keyString);
            var valString = JSON.stringify(req.body);
            console.log("value submitted in POST to be stored in Cache" + valString);

            // Build the args for the request
            var args = {
                data: valString,
                headers: {
                    "Content-Type": "application/octet-stream"
                    , "X-Method": "replace"
                }
            };
            var route_options = {};

            // Issue the POST  -- the callback will return the response to the user
            route_options.method = "POST";
            //            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(keyString);
            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(keyString);
            console.log("Target URL " + route_options.uri);

            route_options.body = args.data;
            route_options.headers = args.headers;



            request(route_options, function (error, rawResponse, body) {
                if (error) {
                    console.log(JSON.stringify(error));
                } else {
                    console.log(rawResponse.statusCode);
                    console.log("BODY:" + JSON.stringify(body));
                    // Proper response is 204, no content.
                    var responseBody = {};
                    if (rawResponse.statusCode == 204) {
                        responseBody['status'] = 'Successful.';
                    }
                    else {
                        responseBody['error'] = 'POST  returned error '.concat(rawResponse.statusCode.toString());
                    }
                    // Send the response
                    res.json(responseBody).end();
                }//else
            });//request
        });//post

        handleAbout = function (req, res) {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.write("Cache-API - About - Version " + settings.APP_VERSION + ". ");
            res.write("Supported URLs:");
            res.write(apiURL + "/key (GET)");
            res.write(apiURL + "/key (POST)");
            res.write(apiURL + "/key (PUT)");
            res.write("incoming headers" + JSON.stringify(req.headers));
            res.end();
        }//handleAbout
    }//registerListeners



        cacheAPI.getFromCache = function (key, callback) {

            var route_options = {};

            // Issue the GET -- the callback will return the response to the user
            route_options.method = "GET";
            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(key);
            console.log("Target URL " + route_options.uri);


            // Issue the GET -- our callback function will return the response
            // to the user
            request(route_options, function (error, rawResponse, body) {
                if (error) {
                    console.log(JSON.stringify(error));
                } else {
                    console.log("rawResponse" + rawResponse.statusCode);
                    console.log("BODY:" + body);
                    // Proper response is 204, no content.
                    var responseBody = {};
                    // If nothing there, return not found
                    responseBody['statusCode'] = rawResponse.statusCode;
                    if (rawResponse.statusCode == 404) {
                        responseBody['error'] = 'Key not found error.';
                        logger.log("*** CacheAPI: failed cache request for key " + key, moduleName, logger.INFO);
                    }
                    else {
                        // Create the response to the caller.
                        responseBody['key'] = key;
                        responseBody['value'] = JSON.parse(body);
                    }
                    // Send the response
                    callback(responseBody);
                }//else
            });//request

        }//getFromCache

        cacheAPI.putInCache = function (key, value, callback) {
            console.log("putInCache Callback = " + callback);
            // Build the args for the request
            var args = {
                data: value,
                headers: { "Content-Type": "application/octet-stream" }
            };
            var route_options = {};

            // Issue the PUT -- the callback will return the response to the user
            route_options.method = "PUT";
            //            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(keyString);
            route_options.uri = baseCCSURL.concat(cacheName).concat('/').concat(key);
            console.log("Target URL " + route_options.uri);

            route_options.body = args.data;
            route_options.headers = args.headers;



            request(route_options, function (error, rawResponse, body) {
                if (error) {
                    console.log(JSON.stringify(error));
                } else {
                    console.log(rawResponse.statusCode);
                    console.log("BODY:" + JSON.stringify(body));
                    console.log("XXXXXXXXXXXXXXXXXXXX  BODY2:" + JSON.stringify(body) + callback);
                    // Proper response is 204, no content.
                    var responseBody = {};
                    if (rawResponse.statusCode == 204) {
                        responseBody['status'] = 'Successful.';
                    }
                    else {
                        responseBody['error'] = 'PUT returned error '.concat(rawResponse.statusCode.toString());
                    }
                    if (callback) { callback(responseBody) }
                }//else
            });//request

        }//putInCache


console.log("Cache API (version " + settings.APP_VERSION + ") initialized at " + apiURL + " running against CACHE Service URL " + baseCCSURL);
