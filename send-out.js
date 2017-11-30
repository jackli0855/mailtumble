const AWS = require('aws-sdk')
const SQS = new AWS.SQS({ apiVersion: '2012-11-05' })
const SES = new AWS.SES()

const QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/103494865495/NeedSendEmailQuence"
const MAXIMUM_SEND_RATE = 30

function sendEmail(message) {
    let data = JSON.parse(message.Body)
    let subject = "A Message To You Rudy"
    if (data.subject !== "") subject = data.subject
    var params = {
        Destination: { ToAddresses: [data.to] },
        Source: data.from,
        Message: {
           Subject: {
              Data: subject
           },
           Body: {
               Text: {
                   Data: data.body,
               }
            }
       }
    }
    
    return new Promise(function(resolve, reject) {
        SES.sendEmail(params, function(err, data) {
            if (err) {
               console.log(err)
               reject(new Error("Error: Email sending failed."))
             } else {
               resolve("Sent email successfully to")
             }
        })
    })
}

function processMessage(message) {
    return sendEmail(message).then(() => {
        const params = {
            QueueUrl: QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
        };

        return new Promise((resolve, reject) => {
            SQS.deleteMessage(params, (err) => {
                if (err) {
                    reject(err)
                } else {
                    resolve()
                }
            });
        })
    })
}

function getQueueData() {
    return new Promise((resolve, reject) => {
        SQS.getQueueAttributes({
            AttributeNames: [
                "ApproximateNumberOfMessages"
            ],
            QueueUrl: QUEUE_URL
        }, function(err, data) {
            if (err) { reject(err) } else resolve(data.Attributes.ApproximateNumberOfMessages);
        });
    })
}

function pollIteration(results) {
  // console.log('========== pollIteration ===========')
    const delay = 0
    const params = {
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 10,
        WaitTimeSeconds: 5
    };

    return new Promise((resolve, reject) => {
        SQS.receiveMessage(params, (err, data) => {
            if (err) return reject(err);

            if (! data.Messages) {
                setTimeout(() => resolve(results.concat('Queue seems to be empty!')), delay)
            } else {
                const promises = data.Messages.map((message) => processMessage(message));
                console.log(`${data.Messages.length} jobs received from the queue`)

                // complete when all invocations have been made
                Promise.series(promises).then(() => {
                    const result = [`Messages processed: ${data.Messages.length}`, data.Messages.length];
                    console.log(result);
                    setTimeout(() => resolve(results.concat(result)), delay)
                });
            }
        });
    });
}

function poll() {
  // console.log('========== poll() ===========')
    const promises = Array(8).fill(pollIteration)

    return Promise.series(promises, [])
}

exports.handler = (event, context, callback) => {
    // TODO implement
    try {
        // Run orchestration (invoked by schedule)
        getQueueData().then((numItems) => {
            console.log(`========= all messages count: ${numItems}`)
            // Choose concurrency level
            const concurrency = Math.min(MAXIMUM_SEND_RATE, Math.max(1, Math.round(parseInt(numItems) / 15)));
            var promises = Array(concurrency);
            for(let i = 0; i < concurrency; i++){
              promises[i] = poll();
            }
            // const promises = Array(concurrency).fill(poll())
            console.log(promises)
            console.log(`Launching ${concurrency} workers`)

            Promise.all(promises).then((results) => {
                // console.log(results)
                let totalMsg = results.reduce((accumulator, currentValue) => {
                  if(Array.isArray(accumulator)){
                    accumulator = accumulator[1] + currentValue[1]
                  }else{
                    accumulator += currentValue[1]  
                  }
                  return accumulator
                });
                callback(null, `end end end all messages count:${totalMsg}`);
            });
        })
    } catch (err) {
        callback(err);
    }
};

Promise.series = function(promises, initValue) {
  console.log('======== Promise.series')
    return promises.reduce(function(chain, promise) {
        console.log(promise)
        // if (typeof promise !== 'function') {
        //     return Promise.reject(new Error("Error: Invalid promise item: " +
        //         promise));
        // }
        return chain.then(promise);
    }, Promise.resolve(initValue));
};