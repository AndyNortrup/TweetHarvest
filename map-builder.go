package main

import (
	"errors"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"golang.org/x/net/html"

	"github.com/ChimeraCoder/anaconda"
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
	longLinkTweets := make(chan LinkTweet, 15)

	retriever := &TweetRetriever{context: mb.c, out: rawTweets}

	var wg sync.WaitGroup
	wg.Add(4)
	go retriever.getTweets(query, cutoff, &wg)
	go mb.extractLinks(rawTweets, shortLinkTweets, &wg)
	go mb.convertAddresses(shortLinkTweets, longLinkTweets, &wg)
	go mb.WriteLinkTweet(longLinkTweets, &wg)

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
	outterWG *sync.WaitGroup) {

	defer outterWG.Done()
	client := urlfetch.Client(mb.c)

	var wg sync.WaitGroup

	for tweet := range links {
		wg.Add(1)
		go func(tweet LinkTweet, out chan<- LinkTweet) {

			defer wg.Done()

			response, err := client.Get(tweet.Address)
			if err != nil {
				log.Infof(mb.c, "Harvester - convertAddress error: %v", err.Error())
				return
			}
			if response.Request.URL.String() != "" {
				tweet.Address = response.Request.URL.String()
				tweet.Query = mb.query
				tweet.Title, err = mb.scrapeTitle(response)
				if err != nil {
					log.Infof(mb.c, err.Error())
				}
				out <- tweet

			}
		}(tweet, out)

	}

	wg.Wait()
	close(out)
}

func (mb MapBuilder) scrapeTitle(resp *http.Response) (string, error) {
	defer resp.Body.Close()

	if strings.ToLower(resp.Header.Get("Content-Type")) != "text/html; charset=utf-8" {
		message := "Wrong content type.  Recieved: " + resp.Header.Get("Content-Type") + " from " + resp.Request.URL.String()
		log.Infof(mb.c, message)
		return "", errors.New(message)
	}

	tokenizer := html.NewTokenizer(resp.Body)
	var titleIsNext bool
	for {
		token := tokenizer.Next()
		switch {
		case token == html.ErrorToken:
			log.Infof(mb.c, "Hit the end of the doc without finding title.")
			return "", errors.New("Unable to find title tag in " + resp.Request.URL.String())
		case token == html.StartTagToken:
			tag := tokenizer.Token()
			isTitle := tag.Data == "title"

			if isTitle {
				titleIsNext = true
			}
		case titleIsNext && token == html.TextToken:
			title := tokenizer.Token().Data
			log.Infof(mb.c, "Pulled title: %v", title)
			return title, nil
		}
	}
}

//WriteLinkTweet writes a given Tweet to the datastore
func (mb MapBuilder) WriteLinkTweet(tweets <-chan LinkTweet, wg *sync.WaitGroup) {
	defer wg.Done()

	var keys []*datastore.Key
	var values []*StoreTweet

	for tweet := range tweets {
		//log.Infof(c, "Putting Tweet into datastore: %v", tweet.Tweet.Id)

		//log.Infof(c, "Key inputs: Kind - %v \t ParentKey: %v", linkTweetKind, getTweetKey(c).String())

		key := datastore.NewIncompleteKey(mb.c, linkTweetKind, getTweetKey(mb.c))
		created, _ := tweet.Tweet.CreatedAtTime()
		/*
			log.Infof(c, "Data: %v, \n\t%v, \n\t%v,\n\t%v,\n\t%v,\n\t%v",
				tweet.Tweet.Text,
				tweet.Address,
				tweet.Tweet.Id,
				created,
				tweet.Tweet.RetweetCount,
				tweet.Tweet.FavoriteCount)
		*/
		store := &StoreTweet{Address: tweet.Address,
			Text:        tweet.Tweet.Text,
			TweetID:     tweet.Tweet.Id,
			CreatedTime: created,
			Retweets:    tweet.Tweet.RetweetCount,
			Favorites:   tweet.Tweet.FavoriteCount,
			Query:       tweet.Query,
			User:        tweet.Tweet.User.Name,
			Title:       tweet.Title,
		}
		keys = append(keys, key)
		values = append(values, store)

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
