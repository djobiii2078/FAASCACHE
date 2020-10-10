const https = require('https');
const sharp = require('sharp');
const AWS = require('aws-sdk');
const fs = require('fs');
const bucketname = "expe-faas";
const { performance } = require("perf_hooks");
const MEMORY_USAGE = 1

const S3 = new AWS.S3({
    accessKeyId : process.env.AWS_ACCESS_KEY,
    secretAccessKey : process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken : process.env.AWS_SESSION_TOKEN,
    params: { Bucket: bucketname }

})


function main(args) {
    args.sigma = isNaN(Number(args.sigma)) ? args.sigma : Number(args.sigma);

    let [extract_time_start, extract_time_stop ]  = [0, 0];
    let [transform_time_start, transform_time_stop ] = [0, 0];
    let [load_time_start, load_time_stop] = [0, 0];

    let [start_time, stop_time] = [0,0];

    start_time = performance.now();

    return new Promise(function(resolve, reject) {
        extract_time_start = performance.now();
        https.get(args.url, (resp) => {
            const { statusCode } = resp;
            extract_time_stop = performance.now(); 
            if (statusCode !== 200) {
                resp.resume();

                console.error('Getting the image failed with status code ' + statusCode);
                reject({msg: 'getting the image failed', statuscode: statusCode});
            }
            transform_time_start = performance.now();
            let data = []

            resp.on('data', (chunk) => { data.push(chunk); })
            resp.on('end', () => {
                let img = sharp(new Buffer.concat(data));

                img = img.blur(args.sigma);
                let fileName = process.env.__OW_ACTIVATION_ID + '.jpg';
                img.toFile(fileName)
                    .then(info => { 
                                    transform_time_stop = performance.now();

                                    let fileBinary = fs.readFileSync(fileName);
                                    let params = {
                                        Body : fileBinary,
                                        Bucket : bucketname,
                                        Key : fileName,
                                        ACL : 'public-read'
                                    }
                                    load_time_start = performance.now();

                                    S3.putObject(params, function(err,data){
                                        if (err) {
                                                    console.error(err);
                                                    reject({msg: 'error'+err});
                                        }
                                        load_time_stop = stop_time = performance.now();
                                        
                                        //  console.log({
                                        //         outputsize: info.size,
                                        //         'elapsed_time' : stop_time - start_time, 
                                        //         'extract_time' : extract_time_stop - extract_time_start,
                                        //         'transform_time' : transform_time_stop - transform_time_start,
                                        //         'load_time' : load_time_stop - load_time_start,
                                        //         'memory_usage' : MEMORY_USAGE ? (process.memoryUsage()['heapUsed'] >> 20) + (process.memoryUsage()['rss'] >> 20) : 'not defined'

                                        // }) 
                                         resolve({
                                                outputsize: info.size,
                                                'elapsed_time' : stop_time - start_time, 
                                                'extract_time' : extract_time_stop - extract_time_start,
                                                'transform_time' : transform_time_stop - transform_time_start,
                                                'load_time' : load_time_stop - load_time_start,
                                                'memory_usage' : MEMORY_USAGE ? (process.memoryUsage()['heapUsed'] >> 20) + (process.memoryUsage()['rss'] >> 20) : 'not defined'
                                            }); 
                                    })
                                  })
                    .catch(err => {
                        console.error('Writing blured image failed:\n' + err);
                        reject({msg: 'writing blured image failed', err: err});
                    });
            });
        }).on('error', (err) => {
            console.error('Error on request:\n' + err.message)
            reject({error: err.message});
        });
    });
}

//  args = {
//     'sigma' : 10,
//     'url' : 'https://expe-faas.s3.amazonaws.com/3.2MB.jpg'
// }

// main(args);
//  
 exports.main = main;
