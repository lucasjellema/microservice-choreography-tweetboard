
var settings = module.exports;
//settings.PORT= 80;  // Note: 5100 for running locally, 80 for running in the cloud
settings.PORT= 5102;  // Note: 5100 for running locally, 80 for running in the cloud

settings.logFile = 'mediator-proxy.txt';

settings.APP_VERSION = "0.1.81";

settings.ChangeHistory =""
+" Logger: get logsDoc from Cache and store to cache; Modified Logger: only publish to Kafka. Added logger-processor: get from Kafka and publish to REST API to write to WLS diagnotics. Added the likes-processor. Added support for SMTP emailing. Text body next to HTML body.";

settings.runLocally = function () {
    return !(process.env.PORT);
}