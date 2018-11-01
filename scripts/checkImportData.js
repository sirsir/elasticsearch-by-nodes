const blobFilePath = "/Users/sirisak/Desktop/SharedLocal/AMIVOICE/projects/inport_file_to_elasticsearch/blob/blob.json"

var fs = require('fs')
    , es = require('event-stream');


var elasticsearch = require('elasticsearch');

const config = require('../configs/config.js');

var client = new elasticsearch.Client(config.elasticsearch.client_connection);

var lineNr = 0;
var voicelogs = []


const checkImportData = {
  streams: {
    imported:{},
    importedFail:{}
  },
  openFiles: function() {
    this.streams.imported = fs.createWriteStream('outputs/elasticsearch_imported.txt');
    this.streams.importedFail = fs.createWriteStream('outputs/elasticsearch_imported_FAIL.txt');
  },
  closeFiles: function() {
    this.streams.imported.end();
    this.streams.importedFail.end();
  },
  // queryTest: function(){
  //   let response = client.search({
  //     index: 'as_voice_logs',
  //     type: 'voice_log',
  //     body: {
  //       query: {
  //         match: {
  //           "recognition_results.result": 'ค่ะ ศูนย์ เจ็ด'
  //         }
  //       }
  //     }
  //   }).then( response=>{
  //     // console.log(response)
  //     for (const tweet of response.hits.hits) {
  //       console.log('tweet:', tweet);
  //     }
  //   })
  // },
  isImported: function(obj) {
    return new Promise( (resolve, reject) =>{
      try {
        client.search({
          index: 'as_voice_logs',
          type: 'voice_log',
          body: {
            query: {
              term: {
                "_id": obj.data._id
              }
            }
          }
        }).then( response=>{
          console.log(response)
          if (response.hits.total == 0){
            reject (obj.line);
          }
          resolve(obj.line);
        })

        
      } catch(e) {
        reject(obj.line);
      }
    })
  },

  getObj: function(l) {
    return new Promise( (resolve, reject) =>{
      try {
        let obj = {
          line: l,
          data: JSON.parse(l)
        }

        resolve(obj)
      } catch(e) {
        reject(l)
      }
    })
  },

  save2imported: function(l) {
    this.streams.imported.write(l + "\n");
  },

  save2importedError: function(l) {
    this.streams.importedFail.write(l + "\n");
  },

  checkImportFromLine: function(l) {
    return new Promise( (resolve, reject) =>{
      try {
        this.getObj(l)
        .then(this.isImported)
        .then(this.save2imported)
        .catch(this.save2importedError)
        .then(resolve)
      } catch(e) {
        reject(e)
      }
    })        
  },
  run: function(){
    // console.log(this)
    this.openFiles()
    // var resultFile = fs.createWriteStream('outputs/elasticsearch_insert_result.txt')


    var s = fs.createReadStream(blobFilePath)
        .pipe(es.split())
        .pipe(es.mapSync(function(line){

            s.pause();

            console.log(++lineNr)

            checkImportData.checkImportFromLine(line)
            .catch()
            .then(s.resume)

            if (lineNr > 3){
              s.destroy();
            }

            
        })
        .on('error', function(err){
            console.log('Error while reading file.', err);
            // resultFile.write('Error while reading file.'+ err+ "\n")
        })
        .on('end', function(){
          console.log('Read entire file.')

          this.closeFiles()

        })
    );
  }

}

checkImportData.run()
// checkImportData.run.apply(checkImportData)
 // setTimeout(checkImportData.run.bind(checkImportData), 1000);
 // var run = checkImportData.run.bind(checkImportData);
 // run()
    
// checkImportData.openFiles()
// var resultFile = fs.createWriteStream('outputs/elasticsearch_insert_result.txt')


// var s = fs.createReadStream(blobFilePath)
//     .pipe(es.split())
//     .pipe(es.mapSync(function(line){

//         s.pause();

//         console.log(++lineNr)

//         checkImportData.checkImportFromLine(line)
//         .catch()
//         .then(s.resume)

//         if (lineNr > 3){
//           s.destroy();
//         }

        
//     })
//     .on('error', function(err){
//         console.log('Error while reading file.', err);
//         resultFile.write('Error while reading file.'+ err+ "\n")
//     })
//     .on('end', function(){
//       console.log('Read entire file.')

//       checkImportData.closeFiles()

//     })
// );

