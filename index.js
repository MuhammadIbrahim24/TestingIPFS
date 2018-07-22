const q1 = 'FILESYSTEM';
const q2 = 'FILESYSTEM_RESPONSE';
const open = require('amqplib').connect('amqp://username:password@ipaddress:port');

const NUMBER_OF_REQUESTS = //no. of requests;

// Publisher
function publisher() {
	open.then(function(conn) {
		return conn.createChannel();
	}).then(function(ch) {
		return ch.assertQueue(q1, {durable: false}).then(function(ok) {
			for(var i = 0; i < 50; i++) {
				ch.sendToQueue(q1, new Buffer(JSON.stringify({
					"type":1,
					"file":fileArray[i],
					"contentType":"text",
					"id":i,
					"key":keyArray[i]
				})));
			}
			return;
		});
	}).catch(console.warn);
	return;
}

// Consumer
function consumer() {
	var i = 0;
	
	var fs = require('fs');    //write values to csv file
	var csvWriter = require('csv-write-stream');
	var writer = csvWriter({sendHeaders: false});
	writer.pipe(fs.createWriteStream('50_requests_async.csv'));
	writer.write({Id: "Id", Hash: "Hash", StartTime: "Start Time", EndTime: "End Time", Difference: "Difference"});
	
	open.then(function(conn) {
		return conn.createChannel();
	}).then(function(ch) {
		return ch.assertQueue(q2, {durable: false}).then(function(ok) {
			return ch.consume(q2, function(msg) {
				if (msg !== null) {
					i++;
					console.log(i);
					console.log(msg.content.toString());
					ch.ack(msg);
					
					
					var obj = JSON.parse(msg.content.toString());
					
					writer.write({Id: obj.id, Hash: obj.Hash, StartTime: obj.startTime, EndTime: obj.endTime, Difference: obj.endTime-obj.startTime});
					if (i == 50) {
						writer.end();
					}
				}
			});
		});
	}).catch(console.warn);
	
	return;
}


//generate string
function makeString(charsArr, strLength) {
	const CHARS = charsArr;
	const STRING_LENGTH = strLength;
	var randomstring = '';
	for (var i = 0; i < STRING_LENGTH; i++) {
		var rnum = Math.floor(Math.random() * CHARS.length);
		randomstring += CHARS.substring(rnum,rnum+1);
	}
	return randomstring;
}

//generate array of string
function makeStringArray(arrLen, charsArr, strLen) {
	var stringArray = [];
	const arrayLength = arrLen;
	if(strLen != 0) {
		for(var i = 0; i < arrayLength; i++) {
			stringArray[i] = makeString(charsArr, strLen);
		}
	}
	else {
		for(var i = 0; i < arrayLength; i++) {
			strLen = Math.floor((Math.random() * 50) + 20);
			stringArray[i] = makeString(charsArr, strLen);
		}
	}
	return stringArray;
}



//Main program
const NO_OF_REQUESTS = 50;
const HEX_CHARS = "0123456789ABCDEF";
const FILE_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

var keyArray = makeStringArray(NO_OF_REQUESTS, HEX_CHARS, 64);
console.log(keyArray);
var fileArray = makeStringArray(NO_OF_REQUESTS, FILE_CHARS, 0);
console.log(fileArray);

publisher();
consumer();
