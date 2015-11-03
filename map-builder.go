package main

import (
	"net/http"
	"sync"

	"golang.org/x/net/context"

	"github.com/AndyNortrup/anaconda"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
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
	shortLinkTweets := make(chan anaconda.Tweet)
	//longLinkTweets := make(chan LinkTweet, 15)

	retriever := &TweetRetriever{context: mb.c, out: rawTweets}

	var wg sync.WaitGroup
	wg.Add(4)
	go retriever.getTweets(query, cutoff, &wg)
	go mb.extractLinks(rawTweets, shortLinkTweets, &wg)

	wg.Wait()
	writer.WriteHeader(http.StatusOK)
}

//extractLinks searches the contents of a tweet and pulls out any url from the
// text.  Results in a channel of LinkTweet objects.
func (mb MapBuilder) extractLinks(tweets <-chan anaconda.Tweet,
	out chan<- anaconda.Tweet,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var inner sync.WaitGroup
	for tweet := range tweets {
		inner.Add(1)
		go func(tweet anaconda.Tweet) {
			defer inner.Done()
			if len(tweet.Entities.Urls) > 0 {
				out <- tweet
			}
		}(tweet)
	}
	inner.Wait()
	close(out)
}

func makeMap(tweets <-chan anaconda.Tweet, wg *sync.WaitGroup) map[string][]int64 {
	var out map[string][]int64

	for tweet := range tweets {
		for _, address := range tweet.Entities.Urls {
			if out[address.Expanded_url] == nil {
				out[address.Expanded_url] = []int64{tweet.Id}
			}
		}
	}
	return out
}

//WriteLinkTweet writes a given Tweet to the datastore
func (mb MapBuilder) writeLinkTweet(tweets <-chan anaconda.Tweet, wg *sync.WaitGroup) {
	defer wg.Done()

	var keys []*datastore.Key
	var values []*int64

	for tweet := range tweets {
		key := datastore.NewIncompleteKey(mb.c, linkTweetKind, getTweetKey(mb.c))
		keys = append(keys, key)
		values = append(values, &tweet.Id)
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
