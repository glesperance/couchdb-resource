var request     = require('request')
  , async       = require('async')
  , querystring = require('querystring')
  , stream      = require('stream')
  , path        = require('path')
  ;

/******************************************************************************
 * 
 * 
 * @param request_args 
 *        The arguments to feed to request
 * 
 * @param query_args (optional)
 *        An object containing the arguments to be passes in the request uri.
 *        
 * @param body_args (optional)
 *        An object containing the arguments to be passed in the request body.
 * 
 * @param callback 
 *        A function of the form function(error, response, body_object)
 *        It is called with the same arguments as if it was passed directly to
 *        request but the body is parsed and checked for errors as well.
 *        
 * @returns the request object (as returned by mikeal's request function)
 */
function do_request() {
  
  var args          = Array.prototype.slice(arguments) 
    
    , request_args  = args.shift()
    
    , pathname      = args.shift()
    
    , callback      = args.pop() 
    
    , query_args    = (   request_args.method.toUpperCase() === 'GET' 
                      ||  args.length == 2 
                      ?   args.shift() : undefined
                      )
    
    , body_args     = (   request_args.method.toUpperCase() !== 'GET' 
                      ||  args.length == 2 
                      ?   args.shift() : undefined
                      )
                      
    , headers       = { 'Authorization': this.auth }
  
    , query_string  = (query_args ? querystring.stringify(query_args) : undefined)
    , body_string   = (body_args ? querystring.stringify(body_args) : undefined)
    , r
    ;
    
    if(request_args.headers) {
      request_args.headers = oo.extend(request_args.headers, headers);
    } else {
      request_args.headers = headers;
    }
    
    request_args.uri = [ this.host
                  , this.port
                  , this.pathname
                  ].join('');
        
    if(query_string) {
      request-args.uri +=  [ '?'
                      , path.join('/', query_string)
                      ].join('');
    }
    
    if(body_string) {
      request_args.body = new Buffer(query_string);
    }
    
    r = request(request_args, function(err, response, body) {
      var body_object
        ;
      
      if(err) { callback(err); return r; }
      
      body_object = JSON.parse(body);
      
      if(body_object.error) {
        callback(body_object);
        return r;
      }
    
      callback(null, response, body_object);
    });
    
    return r;
}

/******************************************************************************
 * CouchDBConnection's Constructor function
 * 
 * Possible parameters:
 *  **host**
 *    The host name or IP address of the database server.
 *    
 *    Default: localhost
 *    
 *  **port**
 *    The port on which the target database server is running.
 *    
 *    Default: 5984
 *    
 *  **options**
 *    The options object.
 *    
 *    Possible options:
 *      **cache**
 *        Enables caching.
 *      **auth**
 *        An auth object consisting of `username`and `password`
 *        
 * @param params {object}
 *        The parameter object
 * 
 * @param callback {function}
 *        A function of the form function(err, connection)Êthat
 *        is called on error or if the CouchDBConnector successfully connected 
 *        to the database.
 * 
 */
function CouchDBConnection(params, callback) {
  var host    = params.host
    , port    = params.port
    , options = params.options
    , req
    ;
  
  this.host = host || 'localhost';
  this.port = port || 5984;
  this.options = options || {};
  
  if(this.options.auth) {
    this.Authorization  = 'Basic ' 
                        + new Buffer
                        ( [ this.options.auth.username
                          , this.options.auth.password
                          ].join(':')
                        ).toString('base64')
  }
  
}
exports.Connection = CouchDBConnection;

