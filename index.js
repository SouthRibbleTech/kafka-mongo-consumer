const { Kafka } = require('kafkajs')
var MongoClient = require('mongodb').MongoClient
var mongoUrl = 'mongodb://root:example@ubuntu:27017'

const kafka = new Kafka({
  clientId: 'mysql_twitter',
  brokers: ['localhost:9092']
})
const consumer = kafka.consumer({ groupId: 'mysql_twitter' })

var dbo = null

MongoClient.connect(mongoUrl, (err, db)=>{
  if(err) throw err
  
  dbo = db.db("twitter")  
})



const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'tweets', fromBeginning: true })
 
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      dbo.collection("tweets").insertOne(JSON.parse(message.value.toString()), (err, res)=>{
        if(err) throw err
        console.log("inserted record into mongo")
      })
    },
  })
}

run().catch(console.error)
