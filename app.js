const cluster = require('cluster')
const path = require('path')
const os = require('os')
const { readdir } = require('fs/promises')
const fs = require('fs')
const csv = require('csv-parser')

async function csvToJson(directoryPath) {

  if (process.argv.length < 3) {
    console.error('Not given the path to the folder');
    process.exit(1);
  }

  if (cluster.isPrimary) {
    const csvFilesArray = await readdir(directoryPath);

    const numCPUs = os.cpus().length;
    const workerNumber = Math.min(numCPUs, csvFilesArray.length);
    let num = 0;

    for (let i = 0; i < csvFilesArray.length; i++) {
      const csvFilePath = path.join(directoryPath, csvFilesArray[i]);

      if (num >= workerNumber) {
        num = 0;
      }

      const worker = cluster.fork();
      worker.send(csvFilePath);
      num++;

      worker.on('message', ({count, duration}) => {
        const pathChunk = csvFilePath.split('\\');
        const fileName = pathChunk[1].replace('.csv', '.json');
        console.log(`It took ${duration} milliseconds to read ${count} lines in ${fileName} file`);
      })
    }
  } else {
    process.on('message', (message) => {
      const filePath = message;

      const startTime = new Date();
      let count = 0;

      const pathChunk = filePath.split('\\');
      const fileName = pathChunk[1].replace('.csv', '.json');

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
          fs.writeFile(`./converted/${fileName}`, JSON.stringify(results, undefined, 2), 'utf-8', (data) => { });
          process.send({ count, duration });
          process.disconnect();
        });
    });
  }
}

csvToJson(process.argv[2]);
