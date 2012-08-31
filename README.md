# Infosquito

Infosquito is a RESTful web service for searching iPlant Collaborative user 
content.


## Configuring Infosquito

Infosquito is configured in the standard way iPlant Collaborative configures web
services.  It pulls it configuration from Zookeeper, with the properties loaded
in Zookeeper by Clavin.

There is a log4j configuration file at /etc/infosquito/log4j.properties.


## Responses

All responses will follow core software's standard response format.  That is, 
unless a server error occurs, or unless otherwise specified, all response bodies
will be a JSON object of the following form.

    {
        "action" : "search",
        "status" : ( "failure" | "success" )
    }

The _status_ property will be "failure" if the request could not be handled,
otherwise it will be "success".

The response object may have more properties specific to a given request.

### Error Responses

When an error occurs on the server side, the response status will be an 
appropriate 500-level HTTP status code.

When an error is detected in the client request, the response body will have
an additional property, "error_code", that is standard text with values 
described shortly.  Here's the full form.

    {
        "action"     : "search",
        "status"     : "failure",
        "error_code" : error code
    }
    
Here are the possible error codes.

* "ERR_INVALID_QUERY_STRING" - This indicates that their is a missing parameter
in the query string, or that the value of one of the parameters is invalid.


## Searching

Infosquito supports searching by name for data files that are readable by a 
given user and inside that user's home folder.  The full file name need not be 
provided; if a glob is provided, all of the names matching the glob will be 
returned.  

Each match result will have a score quantifying how good the match is.  The 
higher the score, the better the match.

Each match will have an index to indicate it position in the result set.  This
is useful for paging.  The first match will have an index of 1.

By default, only the results will be sorted by score in descending order.  By
setting the _sort_ URL parameter to _name_, the results may be lexicographically
sorted by name.

By default, only the first 10 results will be returned.  To return a different
number of results, set the _window_ URL parameter to the desired number of 
results.

The results may be windowed (or paged) as well.  To do this, set the _window_ 
parameter to _n:m_, where _n_ is the index of the first result and _m_ is the
index immediately after the last result to return.  For example, to return the 
results with 11 <= index < 20, set _window_ to _11:20_.   

**NOTE:**  No authentication is performed on the user whose home folder is being
searched.  Authentication should be performed outside of this service.

### Request

To perform a search, a GET request must be sent to the _search_ path at the 
Infosquito end point with a query string of the form:

    search?u=user&n=name\_glob[&sort=field][&window=(limit|from-to)]
          
**Parameters**
* u=_user_ - This is the iPlant Collaborative user name whose home folder will 
be searched.
* n=_name\_glob_ - The results will be files having a name that matches 
_name\_glob_.
* sort=_(_ name _|_ score _)_ - (OPTIONAL) Changes the sort order of the result 
set.  _name_ causes the result set to be sorted lexicographically by matched 
name, while _sort_ causes it to be sorted by score in descending order.
* window=_( limit | from_-_to )_ - (OPTIONAL) Changes the window on the results 
being returned.  _window=limit_ will cause the first _limit_-th, results to be 
returned.  _window=from:to_ will cause the results with index _from_ to _to_ - 1
to be returned.

### Response

The successful response body will be a JSON object of the following form.

    {
        "action"  : "search",
        "status"  : "success",
        "matches" : [
            {
                "index" : numeric index,
                "path"  : the path to the file,
                "name"  : the file name
            }
        ]
    }
