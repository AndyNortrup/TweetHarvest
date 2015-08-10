package main

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/feeds"

	"golang.org/x/net/context"
	"golang.org/x/net/html"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"google.golang.org/appengine/urlfetch"
)

//FeedProducer is a Handler that takes a query and returns a RSS feed
type FeedProducer struct {
	c context.Context
}

//ServeHTTP responds to http requests for the /consume endpoint
func (fp FeedProducer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fp.c = appengine.NewContext(request)

	var rawData chan feeds.Item
	var withTitles chan feeds.Item

	var wg sync.WaitGroup

	rawData = make(chan feeds.Item)
	withTitles = make(chan feeds.Item)

	query, err := getQuery(request, fp.c)
	if err != nil {
		log.Errorf(fp.c, "No query provided.")
		http.Error(writer, "No query provided.", http.StatusBadRequest)
	}
	wg.Add(2)

	go fp.getContentFromDatastore(query, rawData, &wg)
	go fp.getTitles(rawData, withTitles, &wg)

	fp.returnFeed(writer, query, withTitles, &wg)

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
		key, err := i.Next(score)
		if err == datastore.Done {
			break
		}
		if err != nil {
			log.Errorf(fp.c, "Error reading from datastore. %v", err.Error())
		}
		entry := &feeds.Item{
			Link:    &feeds.Link{Href: score.Address},
			Created: score.LastActive,
			Id:      key.String(),
		}
		out <- *entry
	}
	close(out)
}

func (fp FeedProducer) getTitles(in <-chan feeds.Item,
	out chan<- feeds.Item,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var innerWait sync.WaitGroup

	for item := range in {
		log.Infof(fp.c, "Incrementing inner WaitGroup.")
		innerWait.Add(1)
		go func(item feeds.Item) {
			defer innerWait.Done()
			defer log.Infof(fp.c, "Decriment inner wait group by defer.")
			client := urlfetch.Client(fp.c)
			resp, err := client.Get(item.Link.Href)
			log.Infof(fp.c, "Getting title for: %v", item.Link.Href)
			if err != nil {
				log.Errorf(fp.c, "Error retriving page. %v", err.Error())
				return
			}
			if strings.ToLower(resp.Header.Get("Content-Type")) == "text/html; charset=utf-8" {
				title := fp.scrapeTitle(resp)
				item.Title = title
			} else {
				log.Errorf(fp.c, "Wrong content type.  Recieved: %v from %v", resp.Header.Get("Content-Type"), item.Link.Href)
			}
			out <- item
		}(item)
	}
	log.Infof(fp.c, "Waiting for title pull wait group.")
	innerWait.Wait()
	log.Infof(fp.c, "Done waiting for title pull.")
	close(out)
}

func (fp FeedProducer) scrapeTitle(request *http.Response) string {
	defer request.Body.Close()
	tokenizer := html.NewTokenizer(request.Body)
	var titleIsNext bool
	for {
		token := tokenizer.Next()
		switch {
		case token == html.ErrorToken:
			log.Infof(fp.c, "Hit the end of the doc without finding title.")
			return ""
		case token == html.StartTagToken:
			tag := tokenizer.Token()
			isTitle := tag.Data == "title"

			if isTitle {
				titleIsNext = true
			}
		case titleIsNext && token == html.TextToken:
			title := tokenizer.Token().Data
			log.Infof(fp.c, "Pulled title: %v", title)
			return title
		}
	}
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
