const blobFilePath = "/Users/sirisak/Desktop/SharedLocal/AMIVOICE/projects/inport_file_to_elasticsearch/blob/blob.json"

var fs = require('fs')
    , es = require('event-stream');


var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
host: 'http://192.168.1.52:9200',
log: 'trace'
});

var lineNr = 0;
var voicelogs = []


const app_fn ={
  queryTest: function(){
    let response = client.search({
      index: 'as_voice_logs',
      type: 'voice_log',
      body: {
        query: {
          match: {
            "recognition_results.result": 'ค่ะ ศูนย์ เจ็ด'
          }
        }
      }
    }).then( response=>{
      // console.log(response)
      for (const tweet of response.hits.hits) {
        console.log('tweet:', tweet);
      }
    })
  },
  queryImportTest: function(){

  },
  importLine: function(l){
    console.log(lineNr)
    data = JSON.parse(l)
    
    resultFile.write(data._id+ "\n")
    let response = client.create({
      index: 'as_voice_logs',
      type: 'voice_log',
      id: data._id,
      body: data._source
    }).then( response=>{
      // console.log(response)
      resultFile.write(response+ "\n")
    })

  },
  pushVl: function(l){
    try{
      console.log(lineNr)
      data = JSON.parse(l)

      var {keyword_results,recognition_results,recognizer_stats,vergeins,...vl} = data._source
      
      voicelogs.push(vl)
    }catch (e) {
      console.log('Error in pushVl()', e)
    }

  }
}

var resultFile = fs.createWriteStream('outputs/elasticsearch_insert_result.txt')

var s = fs.createReadStream(blobFilePath)
    .pipe(es.split())
    .pipe(es.mapSync(function(line){

        // pause the readstream
        s.pause();

        lineNr += 1;

        // process line here and call s.resume() when rdy
        // function below was for logging memory usage

        // logMemoryUsage(lineNr);
        app_fn.importLine(line);
        // app_fn.pushVl(line);

        // if (lineNr > 3){
        //   s.destroy();
        // }

        // resume the readstream, possibly from a callback
        s.resume();
    })
    .on('error', function(err){
        console.log('Error while reading file.', err);
        resultFile.write('Error while reading file.'+ err+ "\n")
    })
    .on('end', function(){
      console.log('Read entire file.')
      keys = Object.keys(voicelogs[0])
      // console.log(voiceIds.join("\n"))
      values = voicelogs.map(vl=>{
        let arr = []
        keys.forEach(k=>{
          arr.push(vl[k])
        })
        arr = arr.map(v=>`'${v}'`).join(',')
        return `(${arr})`
      }).join(",\n")

      query = `INSERT INTO voicelogs (${keys.join(',')})
      VALUES ${values}
      `
      fs.writeFile('outputs/sql_insert.txt', query, (err) => {  
        // throws an error, you could also catch it here
        if (err) throw err;

        // success case, the file was saved
        console.log('SQL insert query was written to file!');
      });


      ids = voicelogs.map(vl=> vl.id)
      .join(',')

      ids = `(${ids})`

      fs.writeFile('outputs/ids.txt', ids, (err) => {  
        // throws an error, you could also catch it here
        if (err) throw err;

        // success case, the file was saved
        console.log('All id written to file!');
      });
      


    })
);

