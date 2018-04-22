import kafka from "kafka-node"
 
const client = new kafka.Client("localhost:2181");
 
const topics = [
    {
        topic: "webevents4",
        highWaterMark:2,
        offset: 0, //default 0
        partition: 0 // default 0
    }
];
const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "utf8",
    //offset:true
    fromOffset: true,
};

// console.log("here"); 

//const consumer = new kafka.HighLevelConsumer(client, topics, options);

const consumer = new kafka.ConsumerStream(client, topics, options);
//consumer.setEncoding('UTF8');

let data ="";
consumer.on('data', function(chunk,encoding) {
   console.log(encoding);
   if (chunk !== null && typeof chunk === 'object') {
    try { chunk = JSON.stringify(chunk) } catch (e) { next(e) }
  }
  console.log(chunk.toString());
   data += chunk;
   //next(null,chunk)
});

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

const Transform = require('stream').Transform;
const messageTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    // console.log(message.value);
    console.log(encoding);
    console.log(`Received message ${message.value} transforming input`);
    // callback(null, {
    //   topic: 'webevents4',
    //   messages: `You have been (${message.value}) made an example of`
    // });
  }
});

consumer.on("error", function(err) {
    console.log("error", err);
});
 
process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});
// while(1)
// 	consumer.pipe(messageTransform);