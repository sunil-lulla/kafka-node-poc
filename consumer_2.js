import kafka from "kafka-node"
 
const client = new kafka.Client("localhost:2181");
 
const topics = [
    {
        topic: "webevents4"
    }
];
const options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "utf-8"
};
 
const consumer = new kafka.HighLevelConsumer(client, topics, options);
 
consumer.on("message", function(message) {
    
    console.log(message.value);
    // Read string into a buffer.
    var buf = new Buffer(message.value, "binary"); 
    var decodedMessage = JSON.parse(buf.toString());
 
    //Events is a Sequelize Model Object. 
    // return Events.create({
    //     id: decodedMessage.id,
    //     type: decodedMessage.type,
    //     userId: decodedMessage.userId,
    //     sessionId: decodedMessage.sessionId,
    //     data: JSON.stringify(decodedMessage.data),
    //     createdAt: new Date()
    // });
});
 
consumer.on("error", function(err) {
    console.log("error", err);
});
 
process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});