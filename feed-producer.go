package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/feeds"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

//FeedProducer is a Handler that takes a query and returns a RSS feed
type FeedProducer struct {
	c context.Context
}

//ServeHTTP responds to http requests for the /consume endpoint
func (fp FeedProducer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fp.c = appengine.NewContext(request)

	var wg sync.WaitGroup

	rawData := make(chan feeds.Item)

	query, err := getQuery(request, fp.c)
	if err != nil {
		log.Errorf(fp.c, "No query provided.")
		http.Error(writer, "No query provided.", http.StatusBadRequest)
	}
	wg.Add(2)

	go fp.getContentFromDatastore(query, rawData, &wg)

	fp.returnFeed(writer, query, rawData, &wg)

}

func (fp FeedProducer) getContentFromDatastore(query string, out chan<- feeds.Item, wg *sync.WaitGroup) {
	log.Infof(fp.c, "Getting active tweets from the last 7 days.")

	defer wg.Done()

	q := datastore.NewQuery(tweetScoreKind).
		Ancestor(getTweetScoreKey(fp.c)).
		Filter("LastActive >=", time.Now().AddDate(0, 0, -7)).
		Filter("Query =", query).
		Order("-LastActive")

	for i := q.Run(fp.c); ; {

		score := &TweetScore{}
		_, err := i.Next(score)
		if err == datastore.Done {
			break
		}
		if err != nil {
			log.Errorf(fp.c, "Error reading from datastore. %v", err.Error())
		}

		//		out <- *entry
	}
	close(out)
}

func (fp FeedProducer) returnFeed(w http.ResponseWriter,
	query string,
	in <-chan feeds.Item,
	wg *sync.WaitGroup) {

	feed := &feeds.Feed{
		Link:        &feeds.Link{Href: "tweet-integrator.appspot.com/consume?q=golang"},
		Author:      &feeds.Author{Name: "Andy Nortrup", Email: "andrew.nortrup@gmail.com"},
		Description: "Top articles from twitter about: " + query,
		Created:     time.Now(),
		Title:       "Tweet Harvest of topic: " + query,
	}

	for item := range in {
		log.Infof(fp.c, "Added Item to output feed: %v", item.Title)
		feed.Add(&item)
	}

	wg.Wait()
	err := feed.WriteRss(w)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
}
