import kafka from "kafka-node"
 
const client = new kafka.Client("localhost:2181");
 
const topics = [
    {
        topic: "webevents4"
    }
];
const options = {
    autoCommit: false,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024* 1024,
    encoding: "utf-8",
    //offset:true,
    //fromOffset: false,
};

console.log("here"); 

let consumer = new kafka.HighLevelConsumer(client, topics, options);

let notification_batch_max_size=8;
let notification_batch = []; 

consumer.on("message", function(message) {
 	console.log(message);
	if(notification_batch.length < notification_batch_max_size){
 			notification_batch.push(message);
 		}
 	else{
 		
 		function someHeavyActivity(time, callback) {
 			consumer.pause(function(err,data) {});
 			consumer.commit(function(err,data) {
 		
 			})
		    var stop = new Date().getTime();
		    while(new Date().getTime() < stop + time) {
		        ;
		    }
		    console.log(notification_batch);
		    notification_batch = [];
		}
		someHeavyActivity(20000);
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