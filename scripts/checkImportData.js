const blobFilePath = "/Users/sirisak/Desktop/SharedLocal/AMIVOICE/projects/inport_file_to_elasticsearch/blob/blob.json"

var fs = require('fs')
    , es = require('event-stream');


var elasticsearch = require('elasticsearch');

const config = require('../configs/config.js');

var client = new elasticsearch.Client(config.elasticsearch.client_connection);

var lineNr = 0;
var voicelogs = []


const checkImportData ={
  streams: {
    imported:{},
    importedFail:{}
  },
  openFiles: function(){
    checkImportData.streams.imported = fs.createWriteStream('outputs/elasticsearch_imported.txt');
    checkImportData.streams.importedFail = fs.createWriteStream('outputs/elasticsearch_imported_FAIL.txt');
  },
  closeFiles: function(){
    checkImportData.streams.imported.end();
    checkImportData.streams.importedFail.end();
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
  isImported: function(obj){
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
  getObj: function(l){
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
  save2imported:function(l){
    checkImportData.streams.imported.write(l + "\n");
  },
 save2importedError:function(l){
    checkImportData.streams.importedFail.write(l + "\n");
  },
  checkImportFromLine:function(l){
    return new Promise( (resolve, reject) =>{
      try {
        checkImportData.getObj(l)
        .then(checkImportData.isImported)
        .then(checkImportData.save2imported)
        .catch(checkImportData.save2importedError)
        .then(resolve)
      } catch(e) {
        reject(e)
      }
    })
    

    // getObj(l)
    // .then(obj =>{
    //   return checkImportData.isImported(obj);
    // })
    // .then(l => {
    //   checkImportData.save2imported(l);

    // })
    // .catch(l=>{
    //   checkImportData.save2imported(l);
    // })
    
  },
  // importLine: function(l){
  //   console.log(lineNr)
  //   data = JSON.parse(l)
    
  //   resultFile.write(data._id+ "\n")
  //   let response = client.create({
  //     index: 'as_voice_logs',
  //     type: 'voice_log',
  //     id: data._id,
  //     body: data._source
  //   }).then( response=>{
  //     // console.log(response)
  //     resultFile.write(response+ "\n")
  //   })

  // },
  // pushVl: function(l){
  //   try{
  //     console.log(lineNr)
  //     data = JSON.parse(l)

  //     var {keyword_results,recognition_results,recognizer_stats,vergeins,...vl} = data._source
      
  //     voicelogs.push(vl)
  //   }catch (e) {
  //     console.log('Error in pushVl()', e)
  //   }

  // }
}

checkImportData.openFiles()
var resultFile = fs.createWriteStream('outputs/elasticsearch_insert_result.txt')


var s = fs.createReadStream(blobFilePath)
    .pipe(es.split())
    .pipe(es.mapSync(function(line){

        // pause the readstream
        s.pause();

        // lineNr += 1;
        console.log(++lineNr)

        // process line here and call s.resume() when rdy
        // function below was for logging memory usage

        // logMemoryUsage(lineNr);
        // app_fn.importLine(line);
        // app_fn.pushVl(line);
        checkImportData.checkImportFromLine(line)
        .catch()
        .then(s.resume)

        // if (lineNr > 3){
        //   s.destroy();
        // }

        
    })
    .on('error', function(err){
        console.log('Error while reading file.', err);
        resultFile.write('Error while reading file.'+ err+ "\n")
    })
    .on('end', function(){
      console.log('Read entire file.')

      checkImportData.closeFiles()
      // keys = Object.keys(voicelogs[0])
      // // console.log(voiceIds.join("\n"))
      // values = voicelogs.map(vl=>{
      //   let arr = []
      //   keys.forEach(k=>{
      //     arr.push(vl[k])
      //   })
      //   arr = arr.map(v=>`'${v}'`).join(',')
      //   return `(${arr})`
      // }).join(",\n")

      // query = `INSERT INTO voicelogs (${keys.join(',')})
      // VALUES ${values}
      // `
      // fs.writeFile('outputs/sql_insert.txt', query, (err) => {  
      //   // throws an error, you could also catch it here
      //   if (err) throw err;

      //   // success case, the file was saved
      //   console.log('SQL insert query was written to file!');
      // });


      // ids = voicelogs.map(vl=> vl.id)
      // .join(',')

      // ids = `(${ids})`

      // fs.writeFile('outputs/ids.txt', ids, (err) => {  
      //   // throws an error, you could also catch it here
      //   if (err) throw err;

      //   // success case, the file was saved
      //   console.log('All id written to file!');
      // });
      


    })
);

