package tweetharvest

import (
	"net/http"
	"sort"
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
	c     context.Context
	query string
}

//ServeHTTP responds to http requests for the /consume endpoint
func (fp FeedProducer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fp.c = appengine.NewContext(request)

	query, err := getQuery(request, fp.c)
	if err != nil {
		log.Errorf(fp.c, "No query provided.")
		http.Error(writer, "No query provided.", http.StatusBadRequest)
	}
	fp.query = query

	items := make(chan *FeedItem)

	scores := fp.getContentFromDatastore()
	log.Infof(fp.c, "Recieved %v scores.", len(scores))

	go fp.getDescriptions(scores, items)

	fp.returnFeed(writer, items)
}

func (fp FeedProducer) getContentFromDatastore() FeedItems {

	var out FeedItems

	q := datastore.NewQuery(tweetScoreKind).
		Ancestor(getTweetScoreKey(fp.c)).
		Filter("LastActive >=", time.Now().AddDate(0, 0, -7)).
		Distinct().
		Filter("Query =", fp.query).
		Order("-LastActive")

	for i := q.Run(fp.c); ; {
		item := &FeedItem{}
		key, err := i.Next(item)
		if err == datastore.Done {
			break
		}
		if err != nil {
			log.Errorf(fp.c, "Error reading from datastore. %v", err.Error())
		}
		item.key = key.StringID()

		out = append(out, item)
	}
	return out
}

func (fp FeedProducer) getDescriptions(in FeedItems, out chan<- *FeedItem) {
	var wg sync.WaitGroup
	for _, val := range in {
		wg.Add(1)
		go func(item *FeedItem) {
			defer wg.Done()
			item.BuildDescription(fp.c)
			out <- item
		}(val)
	}
	wg.Wait()
	close(out)
}

func (fp FeedProducer) returnFeed(w http.ResponseWriter, in <-chan *FeedItem) {

	feed := &feeds.Feed{
		Link:    &feeds.Link{Href: "http://tweet-integrator.appspot.com/consume?q=golang"},
		Author:  &feeds.Author{Name: "Andy Nortrup", Email: "andrew.nortrup@gmail.com"},
		Title:   "Top articles from twitter about: " + fp.query,
		Updated: time.Now(),
	}

	var scoreItems FeedItems
	for item := range in {
		scoreItems = append(scoreItems, item)
	}

	sort.Sort(scoreItems)
	log.Infof(fp.c, "%v total items to put in feed", len(scoreItems))

	for _, item := range scoreItems {
		feed.Add(item.getItem())
	}

	log.Infof(fp.c, "Writing output.")
	atom, err := feed.ToAtom()
	if err != nil {
		log.Errorf(fp.c, "Error writing ATOM: %v", err.Error())
		return
	}
	//w.Header().Add("Content-Type", "application/atom+xml")
	w.Write([]byte(atom))

}
