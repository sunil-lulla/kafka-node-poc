import kafka from "kafka-node";
import uuid from "uuid";
 
const client = new kafka.Client("localhost:2181", "my-client-id", {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});

let records_to_push = 5;


const producer = new kafka.HighLevelProducer(client);
const KafkaService = {
    sendRecord: ({ type, userId, sessionId, data }, callback = () => {}) => {
        if (!userId) {
            return callback(new Error(`A userId must be provided.`));
        }
 
        const event = {
            id: uuid.v4(),
            timestamp: Date.now(),
            userId: userId,
            sessionId: sessionId,
            type: type,
            data: data
        };
 
        //const buffer = new Buffer.from(JSON.stringify(event));
        for(let i=0;i<records_to_push;i++){
        // Create a new payload
            const record = [
                {
                    topic: "webevents4",
                    messages:  JSON.stringify(event),
                    partition:0,
                    attributes: 1 /* Use GZip compression for the payload */
                }
            ];
     
            //Send record to Kafka and log result/error
            producer.send(record, callback);
        }
    }
};


producer.on("ready", function() {
    console.log("Kafka Producer is connected and ready.");
    KafkaService.sendRecord({type:4,userId:23,sessionId:34},function() {})
    const event = {
            id: uuid.v4(),
            timestamp: Date.now(),
            userId: 1,
            sessionId: 1,
            type: 2,
            data: {}
        };
 
        const buffer = new Buffer.from(JSON.stringify(event));
 
        // Create a new payload
        const record = [
            {
                topic: "webevents4",
                messages: JSON.stringify(event),
                partition:0,
                attributes: 1 
                
                /* 0 No compression */
                /* 1 Use GZip compression for the payload */
                /* 2 Compress using snappy */
            }
        ];
        for(let i=0;i<records_to_push;i++){
            //Send record to Kafka and log result/error
            producer.send(record, function(err,data) {
            	//console.log(err);
                console.log(data);
            });
        }
});
 
// For this demo we just log producer errors to the console.
producer.on("error", function(error) {
    console.error(error);
});


export default KafkaService;



