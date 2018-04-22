// import kafka from "kafka-node"
 
// const client = new kafka.Client("localhost:2181");
 
// const topics = [
//     {
//         topic: "webevents4",
//         highWaterMark:2,
//         offset: 0, //default 0
//         partition: 0 // default 0
//     }
// ];
// const options = {
//     autoCommit: true,
//     fetchMaxWaitMs: 1000,
//     fetchMaxBytes: 1024 * 1024,
//     encoding: "utf8",
//     //offset:true
//     fromOffset: true,
// };

// // console.log("here"); 

// //const consumer = new kafka.HighLevelConsumer(client, topics, options);

// const consumer = new kafka.ConsumerStream(client, topics, options);
// consumer.setEncoding('UTF8');

// let data ="";
// consumer.on('data', function(chunk) {
//    console.log(chunk);
//    data += chunk;
// });

// consumer.on("message", function(message) {
//  	//console.log("here");
//  	console.log(message);
//     // Read string into a buffer.
//   	// var buf = new Buffer(message.value, "binary"); 
//   	// var decodedMessage = JSON.parse(buf.toString());
//  	//console.log(decodedMessage);
//     //Events is a Sequelize Model Object. 
//     // return Events.create({
//     //     id: decodedMessage.id,
//     //     type: decodedMessage.type,
//     //     userId: decodedMessage.userId,
//     //     sessionId: decodedMessage.sessionId,
//     //     data: JSON.stringify(decodedMessage.data),
//     //     createdAt: new Date()
//     // });
// });
 
// consumer.on("error", function(err) {
//     console.log("error", err);
// });
 
// process.on("SIGINT", function() {
//     consumer.close(true, function() {
//         process.exit();
//     });
// });

import kafka from "kafka-node";
const ConsumerGroupStream = new kafka.ConsumerGroupStream;
const Transform = require('stream').Transform;
const resultProducer = new kafka.ProducerStream
 
const consumerOptions = {
  kafkaHost: '127.0.0.1:9092',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  asyncPush: false,
  id: 'consumer1',
  fromOffset: 'latest',
  highWaterMark:10
};
 
let topics = ['webevents4']
//const consumerGroup = new ConsumerGroupStream(consumerOptions,topics);
//const consumerGroup = new ConsumerGroupStream(consumerOptions, 'ExampleTopic');


let consumerGroup = new kafka.ConsumerGroupStream(consumerOptions,topics);


const messageTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    // console.log(message.value);
    console.log(`Received message ${message.value} transforming input`);
    // callback(null, {
    //   topic: 'webevents4',
    //   messages: `You have been (${message.value}) made an example of`
    // });
  }
});
 
consumerGroup.pipe(messageTransform).pipe(resultProducer);