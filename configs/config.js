const config ={
  elasticsearch:{
    client_connection: {
      host: 'http://192.168.1.52:9200',
      // log: 'trace'
      log: ['error', 'warning']
    }
  }
}

module.exports = config;