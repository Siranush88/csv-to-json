//import path from 'path';
// import fs from 'fs';
// import csv from 'csv-parser';
// import { writeFile } from 'fs/promises';
//import { readdir } from 'fs/promises';
//import cluster from 'cluster';
//import os from 'os';
const cluster = require('cluster')
const path = require('path')
const os = require('os')
//const readdir =  require('fs/promises')
const fs = require('fs')
const csv = require('csv-parser')

if (cluster.isPrimary) {

    const directoryPath = process.argv[2];
    if (process.argv.length < 3) {
        console.error('Not given the path to the folder');
        process.exit(1);
    }

    const csvFilesArray = fs.readdirSync(directoryPath);
    const numCpus = os.cpus().length;

    let worker;

    
    for (let i = 0; i < csvFilesArray.length; i++) {
        worker = cluster.fork();
        
    }

    worker.on('online', (msg) => {
        for (let i = 0; i < csvFilesArray.length; i++) {
           let csvFilePath = path.join(directoryPath, csvFilesArray[i]);
            worker.send(csvFilePath);
        }
    })
} else {

    process.on('message', (message) => {
        const filePath = message;

        const startTime = new Date();
        let count = 0;

        const pathChunk = filePath.split('\\')
        const fileName = pathChunk[1].replace(".csv", ".json");

        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => {
                count++;
                results.push(data);
            })
            .on('end', () => {
                const endTime = new Date();
                const duration = endTime - startTime;
                console.log(`Parsing ${fileName} took ${duration} milliseconds to read ${count} lines`);
                fs.writeFile(`./converted/${fileName}`, JSON.stringify(results, undefined, 2), 'utf-8', (data) => { })
            })
    });
}