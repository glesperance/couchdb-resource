
/******************************************************************************
 * 
 * @param obj
 * @param callback
 * @returns
 */
CouchDBConnection.create = function create_CouchDBConnection(obj, callback) {
      var constructor = this.prototype.constructor
        , obj = (obj instanceof constructor ? obj : new constructor(obj))
        ;
        
      obj.save(callback);  
    }

/******************************************************************************
 * 
 */
CouchDBConnection.save = function save_CouchDBConnection(obj, callback) {
      var constructor = this.prototype.constructor
        , obj = (obj instanceof constructor ? obj : new constructor(obj))
        ;
        
      obj.save(callback);  
    }

/******************************************************************************
 * 
 * @param _id
 * @param callback
 */
CouchDBConnection.get = function get_CouchDBConnection(_id, callback) {
  var that = this;
  this.__db.get(_id, function(err, doc){ objectify(doc, { view: false }, that.prototype.constructor, callback); });
}

/******************************************************************************
 * 
 * @param _id
 * @param callback
 */
CouchDBConnection.destroy = function destroy_CouchDBConnection(_id, callback) {
  this.__db.remove(_id, callback);
}

/******************************************************************************
 * 
 * @param args
 */
CouchDBConnection.all = function all_CouchDBConnection(args) {
  var args_array = Array.prototype.slice.call(arguments)
    ;
    
  args_array.unshift('rocket/all');
  
  this.view.apply(this, args_array);
}

/******************************************************************************
 * 
 * @param options
 * @param callback
 * @returns {___anonymous3742_3777}
 */
CouchDBConnection.changes = function changes_CouchDBConnection(options, callback) {
  var promise = new(events.EventEmitter)
    , params = options
    , params_array = []
    , v
    , auth
    , get_options
    ;

   for(var k in params) {
     v = params[k];
  
     params_array.push( [ k , '=' , v ].join('') );
   }
   
   auth = 'Basic ' + new Buffer(this.connection.options.auth.username + ':' + this.connection.options.auth.password).toString('base64')
     , get_options   = {
         headers: { 'Authorization': auth }
       , host: this.connection.host
       , options: this.connection.options
       , port: this.connection.port
       , path: 
         [
           '/' 
         , path.join(
            this.db_name
           , '_changes'
           )
         , '?'
         , params_array.join('&')
         ].join('')
        } 
      ;

    if (callback) {
      //this.query('GET', '_changes', options, callback);
      http.get(get_options, callback);
    } else {

        //that.rawRequest('GET', [name, '_changes'].join('/'), options).on('response', function (res) {
        var request = http.get(get_options, function(res) {
          var response = new(events.EventEmitter), buffer = [];
            res.setEncoding('utf8');

            response.statusCode = res.statusCode;
            response.headers    = res.headers;
            
            promise.emit('response', response);

            res.on('data', function (chunk) {
                if (chunk.trim()) {
                    buffer.push(chunk);

                    if (chunk.indexOf('\n') !== -1) {
                        buffer.length && response.emit('data', JSON.parse(buffer.join('')));
                        buffer = [];
                    }
                }
            }).on('end', function () {
                response.emit('end');
            });
        });
        
        return {promise: promise, request: request};
    }
} 

/******************************************************************************
 * 
 */
CouchDBConnection.view = function myView_CouchDBConnection() {
  var args_array    = Array.prototype.slice.call(arguments)
    , view          = args_array.shift()
    , callback      = args_array.pop()
    , params        = (  
                         typeof args_array[0] === 'object' 
                      && args_array[0] 
                      && ! Array.isArray(args_array[0])
                      ? args_array.shift() 
                      : {}
                      )
    , params_array = [];
    
  for(var k in params) {
    v = params[k];
    
    params_array.push([
                        k
                      , '='
                      , JSON.stringify(v)
                      ].join('')
    );
  }
    
  var constructors  = (  
                       typeof args_array[0] === 'object' 
                    && args_array[0] 
                    && Array.isArray(args_array[0])
                    ? args_array.shift() 
                    : this.prototype.constructor
                    )
    , splitted_param = view.split('/')
    , auth = 'Basic ' + new Buffer(this.connection.options.auth.username + ':' + this.connection.options.auth.password).toString('base64')
    , get_options   = {
        headers: { 'Authorization': auth }
      , host: this.connection.host
      , options: this.connection.options
      , port: this.connection.port
      , path: [
                '/' 
              , path.join(
                this.db_name
              , '_design'
              , (splitted_param.length === 1  ? this.prototype.doc_type : splitted_param[0])
              , '_view'
              , (splitted_param.length === 1  ? splitted_param[0] : splitted_param[1])
              )
            , '?'
            , params_array.join('&')
            ].join('')
      } 
    ;
    
  var buffer = '';
  http.get(get_options, function(res) {
    
    res.on('data', function(chunk) {
      buffer += chunk;
    });
    
    res.on('end', function() {
      var obj = JSON.parse(buffer);
      
      objectify(obj.rows, {view: true, include_docs: params.include_docs}, constructors, function(err, objects){
        obj.rows = objects;
        callback(err, obj);
      });
    });
    
    res.on('error', function(err) {
      console.log('ERROR: ' + err);
    });
    
  }).on('error', function(err) {
    console.log('CON ERROR: ' + err)
  });
}