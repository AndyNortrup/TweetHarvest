package main

import (
	"errors"
	"net/http"
	"sync"

	"golang.org/x/net/context"

	"github.com/AndyNortrup/anaconda"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

//MapBuilder is a microservice that querries the Twitter API and gets copies
// of all of the tweets with a specified string from the query string.
type MapBuilder struct {
	c     context.Context
	query string
}

//ServeHTTP recives and processes a request from the web.  Expects a parameter
//q which is the query string.
func (mb MapBuilder) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//Create a context
	mb.c = appengine.NewContext(request)
	log.Infof(mb.c, "Starting Tweet Harvest.")

	//Get the query string and validate that it is not an error
	query, err := getQuery(request, mb.c)
	if err != nil {
		log.Errorf(mb.c, "Failed to get query from querystring.")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
	mb.query = query

	cutoff := getNewestTweet(mb.c)
	log.Infof(mb.c, "Newest Tweet in datastore is dated: %v", cutoff.String())

	rawTweets := make(chan anaconda.Tweet)
	shortLinkTweets := make(chan LinkTweet)
	//longLinkTweets := make(chan LinkTweet, 15)

	retriever := &TweetRetriever{context: mb.c, out: rawTweets}

	var wg sync.WaitGroup
	wg.Add(4)
	go retriever.getTweets(query, cutoff, &wg)
	go mb.extractLinks(rawTweets, shortLinkTweets, &wg)
	//go mb.convertAddresses(shortLinkTweets, longLinkTweets, &wg)
	go mb.writeLinkTweet(shortLinkTweets, &wg)

	wg.Wait()
	writer.WriteHeader(http.StatusOK)
}

//extractLinks searches the contents of a tweet and pulls out any url from the
// text.  Results in a channel of LinkTweet objects.
func (mb MapBuilder) extractLinks(tweets <-chan anaconda.Tweet,
	out chan<- LinkTweet,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var inner sync.WaitGroup
	for tweet := range tweets {
		inner.Add(1)
		go func(tweet anaconda.Tweet) {
			defer inner.Done()
			linkTweet, err := LinkTweetFrom(tweet)
			if err == nil {
				linkTweet.Query = mb.query
				out <- linkTweet
			}
		}(tweet)
	}
	inner.Wait()
	close(out)
}

//convertAddresses converts shortend addresses to their original format.
func (mb MapBuilder) convertAddresses(links <-chan LinkTweet,
	out chan<- LinkTweet,
	wg *sync.WaitGroup) {

	defer wg.Done()
	client := urlfetch.Client(mb.c)

	var innerWg sync.WaitGroup

	for tweet := range links {
		innerWg.Add(1)
		go func(tweet LinkTweet, out chan<- LinkTweet) {

			defer innerWg.Done()

			response, err := client.Head(tweet.Address)
			if err != nil {
				log.Infof(mb.c, "Harvester - convertAddress error: %v", err.Error())
				return
			}
			if response.Request.URL.String() != "" {
				tweet.Address = response.Request.URL.String()
				tweet.Query = mb.query
				out <- tweet

			}
		}(tweet, out)

	}

	innerWg.Wait()
	close(out)
}

//WriteLinkTweet writes a given Tweet to the datastore
func (mb MapBuilder) writeLinkTweet(tweets <-chan LinkTweet, wg *sync.WaitGroup) {
	defer wg.Done()

	var keys []*datastore.Key
	var values LinkTweets

	for tweet := range tweets {
		//log.Infof(c, "Putting Tweet into datastore: %v", tweet.Tweet.Id)

		//log.Infof(c, "Key inputs: Kind - %v \t ParentKey: %v", linkTweetKind, getTweetKey(c).String())

		key := datastore.NewIncompleteKey(mb.c, linkTweetKind, getTweetKey(mb.c))

		keys = append(keys, key)
		values = append(values, &tweet)

		if key == nil {
			err := errors.New("Key is nil befor put.")

			log.Criticalf(mb.c, "%v", err.Error())
			return
		}
	}
	err := datastore.RunInTransaction(mb.c, func(c context.Context) error {
		_, err := datastore.PutMulti(c, keys, values)

		if err != nil {
			log.Errorf(c, "Failed to write LinkTweet to datastore. %v", err.Error())
			return err
		}
		return nil
	}, nil)
	if err != nil {
		log.Errorf(mb.c, "Failed to write LinkTweet to datastore. %v", err.Error())
	}
}
