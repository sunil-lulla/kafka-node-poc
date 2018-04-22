import kafka from "kafka-node"
 
const client = new kafka.Client("localhost:2181");
 
var topics = [
    {
        topic: "webevents4"
    }
];
var options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: "utf-8"
};

options = {
  host: 'zookeeper:2181',  // zookeeper host omit if connecting directly to broker (see kafkaHost below)
  kafkaHost: 'localhost:9092', // connect directly to kafka broker (instantiates a KafkaClient)
  zk : undefined,   // put client zk settings if you need them (see Client)
  batch: undefined, // put client batch settings if you need them (see Client)
  ssl: true, // optional (defaults to false) or tls options hash
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  // An array of partition assignment protocols ordered by preference.
  // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
  protocol: ['roundrobin'],
 
  // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
  // equivalent to Java client's auto.offset.reset
  fromOffset: 'latest', // default
 
  // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
  outOfRangeOffset: 'earliest', // default
  migrateHLC: false,    // for details please see Migration section below
  migrateRolling: true,
  // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
  // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
  onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
};

topics = [
    "webevents4"
];


const consumer = new kafka.ConsumerGroup(options, topics);
let notification_batch_max_size=8;
let notification_batch = []; 

consumer.on("message", function(message) {
    // consumer.commit(function(err,data) {
    //     //console.log(err);
    //     console.log(data);
    // });
    //console.log(notification_batch.length);
    if(notification_batch.length < notification_batch_max_size){
            notification_batch.push(message);
        }
    else{
        function someHeavyActivity(time) {
            // consumer.pause(function(err,data) {});
            // consumer.commit(function(err,data) {
        
            // })

            //
            var stop = new Date().getTime();
            while(new Date().getTime() < stop + time) {
                consumer.pause();  
                //console.log("inside loop "+notification_batch.length);
            }
            console.log(notification_batch);
            notification_batch = [];
            consumer.resume();
        }
        someHeavyActivity(4000);
        
    }


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