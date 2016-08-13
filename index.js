var AWS = require('aws-sdk')
var ChangesStream = require('changes-stream')
var to = require('flush-write-stream')

;['AWS_BUCKET', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
.forEach(function (key) {
  if (!process.env[key]) {
    console.error('No ' + key)
    process.exit(1)
  }
})

var AWS_BUCKET = process.env.AWS_BUCKET
var AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID
var AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY

var s3 = new AWS.S3({
  apiVersion: '2006-03-01',
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY
})

new ChangesStream({
  db: 'https://replicate.npmjs.com',
  include_docs: true,
  since: process.argv[2] ? parseInt(process.argv[2]) : 0
})
.on('error', function (error) {
  console.error(error)
})
.pipe(to.obj(function (data, _, done) {
  var sequence = data.seq
  var params = {
    Bucket: AWS_BUCKET,
    Key: String(sequence),
    Body: JSON.stringify(data)
  }
  s3.upload(params, function (error) {
    if (error) {
      console.error('%d error: %s', sequence, error)
    } else {
      console.log('Uploaded %d', sequence)
    }
    done()
  })
}))
