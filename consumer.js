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
    fetchMaxBytes: 100,
    encoding: "utf-8",
    //offset:true
    fromOffset: false,
};

// console.log("here"); 

const consumer = new kafka.Consumer(client, topics, options);

let notification_batch_max_size=5;
let notification_batch = []; 


consumer.on("message", function(message) {
 	
	if(notification_batch.length<notification_batch_max_size){
 			notification_batch.push(message);
 		}
 	else{
 		
 		function someHeavyActivity(time, callback) {
 			consumer.close(function(err,data) {});
 			consumer.commit(function(err,data) {
 		
 			})
		    var stop = new Date().getTime();
		    while(new Date().getTime() < stop + time) {
		        ;
		    }
		    notification_batch = [];
		}
		someHeavyActivity(20000);
});
 
consumer.on("error", function(err) {
    console.log("error", err);
});
 
process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});