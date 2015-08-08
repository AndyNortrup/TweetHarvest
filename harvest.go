package main

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"golang.org/x/net/context"

	"github.com/ChimeraCoder/anaconda"
	"github.com/mvdan/xurls"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

//TweetHarvester is a microservice that querries the Twitter API and gets copies
// of all of the tweets with a specified string from the query string.
type TweetHarvester struct{}

const queryParam string = "q"

func init() {
	th := &TweetHarvester{}
	proc := &TweetProcessor{}

	plex := mux.NewRouter()
	plex.Handle("/harvest", th)
	plex.Handle("/process", proc)
	http.Handle("/", plex)

}

//ServeHTTP recives and processes a request from the web.  Expects a parameter
//q which is the query string.
func (th TweetHarvester) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//Create a context
	c := appengine.NewContext(request)
	log.Infof(c, "Starting Tweet Harvest.")

	//Get the query string and validate that it is not an error
	query, err := getQuery(c, request)
	if err != nil {
		log.Errorf(c, "Failed to get query from querystring.")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	log.Infof(c, "Newest Tweet in datastore is dated: %v", getNewestTweet(c).String())

	rawTweets := make(chan anaconda.Tweet)
	shortLinkTweets := make(chan LinkTweet)
	longLinkTweets := make(chan LinkTweet, 15)
	semaphor := make(chan int)

	go getTweets(c, query, rawTweets)
	go extractLinks(c, rawTweets, shortLinkTweets)
	go convertAddresses(c, shortLinkTweets, longLinkTweets)
	go WriteLinkTweet(c, longLinkTweets, semaphor)

	<-semaphor

	writer.WriteHeader(http.StatusOK)
}

//getQuery pulls the query from the q paramenter of the query string
func getQuery(c context.Context, request *http.Request) (string, error) {
	result := request.URL.Query().Get(queryParam)

	if result == "" {
		return "", errors.New("No query specified.")
	}
	log.Infof(c, "Recived query parameter: %v", result)
	return result, nil
}

//getTweets gets all tweets from twitter with the speified keyword
func getTweets(c context.Context, query string, out chan anaconda.Tweet) {
	log.Infof(c, "Downloading Tweets.")
	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecretKey)

	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)

	api.HttpClient.Transport = &urlfetch.Transport{Context: c}

	result, err := api.GetSearch(query, nil)

	if err != nil {
		log.Errorf(c, "Harvester- getTweets: %v", err.Error())
		return
	}
	cont := true

	for cont {
		cont = addIfNewerThan(getNewestTweet(c), result, out)
		cont = false
		if cont {
			result, err = result.GetNext(api)
			//log.Infof(c, "Getting more tweets!")
			if err != nil {
				log.Errorf(c, "Harvester- getTweets: %v", err.Error())
			}
		}
	}
	close(out)
}

func addIfNewerThan(cutoff time.Time, result anaconda.SearchResponse, output chan anaconda.Tweet) bool {
	cont := true
	for _, tweet := range result.Statuses {
		if time, _ := tweet.CreatedAtTime(); time.After(cutoff) {
			output <- tweet
		} else {
			cont = false
			break
		}
	}
	return cont
}

//extractLinks searches the contents of a tweet and pulls out any url from the
// text.  Results in a channel of LinkTweet objects.
func extractLinks(c context.Context, tweets <-chan anaconda.Tweet, out chan<- LinkTweet) {

	for tweet := range tweets {

		rawAddr := xurls.Strict.FindString(tweet.Text)

		if rawAddr != "" {
			//log.Infof(c, "Extracted Link: %v", rawAddr)
			linkTweet := LinkTweet{
				Address: rawAddr,
				Tweet:   tweet,
			}
			out <- linkTweet
		}
	}
	close(out)
}

//convertAddresses converts shortend addresses to their original format.
func convertAddresses(c context.Context, links <-chan LinkTweet, out chan<- LinkTweet) {
	client := urlfetch.Client(c)

	var wg sync.WaitGroup

	for tweet := range links {
		wg.Add(1)
		go func(tweet LinkTweet, out chan<- LinkTweet) {

			defer wg.Done()

			response, err := client.Head(tweet.Address)
			if err != nil {
				log.Infof(c, "Harvester - convertAddress error: %v", err.Error())
				return
			}
			tweet.Address = response.Request.URL.String()
			out <- tweet
		}(tweet, out)

	}

	wg.Wait()
	close(out)
}
