##TweetHarveser

TweetHarveser is a very simple example project that runs a map-reduce process on data gathered from twitter.  

It uses the anaconda twitter library (https://github.com/ChimeraCoder/anaconda) to access the twitter API. The project is designed to run on Google App Engine  (https://cloud.google.com/appengine/) and it's accompanying datastore.

##/map

The process endpoint provides the map function to the process  and takes a keyword in a query string (i.e. /harvest?q=golang) then searches twitter for that keyword.  All returned tweets are scraped for URLs.  Short URLs are converted to their full length versions then stored in the Google App Engine Datastore.

##/reduce

The process endpoint takes serves as a reduce function to the process.  It takes all tweets gathered by harvest and gives them a score (1 + favorites).  The address, score and a list of all associated TweetIDs are stored into the Datastore.

##/consume (yet to be written)

The consume endpoint takes a keyword, and returns a popularity sorted RSS feed of the most links gathered for that keyword.
