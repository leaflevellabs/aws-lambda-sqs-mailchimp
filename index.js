var aws = require( "aws-sdk"),
		rest = require('restler'),
		configFile = require("./config.json");

// get configuration defaults from config file.
var mc_newsletterid = configFile.mailChimpNewsletterId;
var mc_userName = configFile.mailChimpUserName;
var mc_password = configFile.mailChimpPassword;
var mc_dc = configFile.mailChimpDC;
var queueUrl = configFile.queueUrl;
var configLocation = configFile.configLocation;

var dbClient = new aws.DynamoDB.DocumentClient();
var sqsClient = new aws.SQS();

// get config values from dynamodb - if the config values are found, then override existing values
// this will occur on every execution of the lambda which will allow real time configuration changes.
var updateConfig = function updateConfigValues(invokedFunction, cb) {

	var version = invokedFunction.split(":").pop();

	var params = {
		TableName: configLocation,

		Key : {
			"service" : "lambda",
			"pointer" : process.env.AWS_LAMBDA_FUNCTION_NAME + ":" + version
		}
	};

	dbClient.get(params, function(err, data) {
		console.log(err);
		console.log(data);
		if(err) {
			console.log("ERR_DYNAMODB_GET", err, params);
		}
		else if(!data || !data.Item || !data.Item.config) {
			console.log("INFO_DYNAMODB_NOCONFIG", params);
		}
		else {
			mc_newsletterid = data.Item.config.mailChimpNewsletterId || mc_newsletterid;
			mc_userName = data.Item.config.mailChimpUserName || mc_userName;
			mc_password = data.Item.config.mailChimpPassword || mc_password;
			mc_dc = data.Item.config.mailChimpDC || mc_dc;
			queueUrl = data.Item.config.queueUrl || queueUrl;
			console.log("INFO_DYNAMODB_CONFIG", "success");
		}

		return cb(err);
	});

}

// add the email address to the mailchimp list.
var subscribeEmail = function saveEmail(email, cb) {

	var url = "https://" +  mc_dc + ".api.mailchimp.com/3.0/lists/" + mc_newsletterid + "/members" ;

	rest.post(url, {
		username: mc_userName,
		password: mc_password,
		data: JSON.stringify({
			status: "subscribed",
			email_address: email
		}),
		timeout: 10000
	})
	.on('timeout', function(ms){
		var toError = new Error();
		toError.name = "HTTP Request Timeout";
		return cb(toError);
	})
	.on('success', function(data, response) {
		return cb();
	})
	.on('fail', function(data, response) {
		var returnData = data ? JSON.parse(data) : {};
		if(returnData.title && returnData.title === "Member Exists") {
			return cb();
		}
		else {
			var error = new Error(returnData.status);
			error.name = "HTTP Request Fail";
			error.message = returnData.detail;
			return cb(error);
		}

	})
	.on('error', function(err, response) {
		return cb(err);
	});
}

var deleteMessage = function deleteMessage(receiptHandle, cb) {

	var params = {
		QueueUrl: queueUrl,
		ReceiptHandle: receiptHandle
	};

	sqsClient.deleteMessage(params, function(err, data) {
		cb(err, data);
	});

}

exports.handler = function(event, context) {

	updateConfig(context.invokedFunctionArn, function(err) {

		if(err) {
			context.done(err);
			return;
		}

		console.log("INFO_LAMBDA_EVENT", event);
		console.log("INFO_LAMBDA_CONTEXT", context);

		console.log("QUEUEURL", queueUrl);
		sqsClient.receiveMessage({MaxNumberOfMessages: 5, QueueUrl: queueUrl}, function(err, data) {

			if(err) {
				console.log("ERR_SQS_RECEIVEMESSAGE", err);
				context.done(err);
			}
			else {

				if (data && data.Messages) {

					var msgCount = data.Messages.length;

					console.log("INFO_SQS_RESULT", msgCount + " messages received");

					for(var x=0; x < msgCount; x++) {

						var message = data.Messages[x];
						console.log("INFO_SQS_MESSAGE", message);
						var messageBody = JSON.parse(message.Body);

						subscribeEmail(messageBody.Message, function(err, data) {

							if (err) {
								console.error("ERR_MAILCHIMP_SUBSCRIBE", "receipt handle: " + message.ReceiptHandle, err);
								context.done(err);
							}
							else {
								console.log("INFO_MAILCHIMP_SUBSCRIBE", "success", "receipt handle: " + message.ReceiptHandle, messageBody.Message);
								deleteMessage(message.ReceiptHandle, function(err) {
									if(!err) {
										console.error("INFO_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, "successful");
									} else {
										console.error("ERR_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, err);
									}
									context.done(err);
								});
							}

						});
					}
				} else {
					console.log("INFO_SQS_RESULT", "0 messages received");
					context.done(null);
				}
			}
		});
	});

}