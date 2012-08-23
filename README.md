# Infosquito

Infosquito is a RESTful web service for searching iPlant Collaborative user 
content.


## Configuring Infosquito

**TODO describe how to configure Infosquito.**


## Responses

All responses will follow core software's standard response format.  That is, 
unless a server error occurs, or unless otherwise specified, all response bodies
will be a JSON object of the following form.

    {
        "action" : "search",
        "status" : ( "failure" | "success" )
    }

The "status" property will be "failure" if the request could not be handled,
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

**TODO list the error codes and what they mean**


## Searching

Infosquito supports searching by name for data folders that are readable by a 
given user and inside that user's home folder.  The full folder name need not be 
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

The results may be paged (or windowed) as well.  To do this, set the _window_ 
parameter to _n:m_, where _n_ is the index of the first result and _m_ is the
index of the last result to return.  For example, to return the 11th through the 
20th results, set _window_ to _11:20_.   

**NOTE:**  No authentication is performed on the user whose home folder is being
searched.  Authentication should be performed outside of this service.

### Request

To perform a search, a GET request must be sent to the _search_ path at the 
Infosquito end point with a query string of the form:

    search?u=user&n=name-glob[&sort=field][&window=(limit|from:to)]
          
**Parameters**
* u=_user_ - This is the iPlant Collaborative user name whose home folder will 
be searched.
* n=_name-glob_ - The results will be folders having a name that matches 
_name_glob_.
* sort=_(_name_|_score_)_ - (OPTIONAL) Changes the sort order of the result set.
_name_ causes the result set to be sorted lexicographically by matched name, 
while _sort_ causes it to be sorted by score in descending order.
* window=_(limit|from_:_to)_ - (OPTIONAL) Changes the window on the results 
being returned.  _window=limit_ will cause the first through limit-th, 
inclusive, results to be returned.  _window=from:to_ will cause the from-th 
through the to-th, inclusive, result to be returned.

### Response

If successful, up to 10 matches will be returned.  The matches will be sorted by
score in descending order.

The successful response body will be a JSON object of the following form.

    {
        "action"  : "search",
        "status"  : "success",
        "matches" : [
            {
                "index" : numeric index,
                "score" : numeric score,
                "path"  : the path to the folder,
                "name"  : the folder name
            }
        ]
    }
