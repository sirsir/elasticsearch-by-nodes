const fs = require('fs')
    , es = require('event-stream');


const elasticsearch = require('elasticsearch');

const config = require('../configs/config.js');

const client = new elasticsearch.Client(config.elasticsearch.client_connection);

const data = {
  keywords: [
    
  ]
}


const findKeywords = {
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

  isMatch: function(textIn){
    if (textIn.indexOf('ขั้น ต่ำ') != -1){
      return true;
    }

    return false;

  },

  run: function(){
      client.search({
      index: 'as_voice_logs',
      type: 'voice_log',
      body: {
        query: {
          match: {
            "recognition_results.result": 'ขั้น ต่ำ'
          }
        }
      }
    }).then( response=>{
      // console.log(response)
      for (const hits of response.hits.hits) {
        // console.log('tweet:', JSON.stringify(hits));
        hits._source.recognition_results.forEach(r=>{
          if (this.isMatch(r.result)){
            console.log(r.result)
          }
        })
      }
    })
    
  }

}

findKeywords.run()


