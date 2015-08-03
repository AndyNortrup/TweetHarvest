package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"tweetdata"

	"github.com/gorilla/mux"

	"golang.org/x/net/context"

	"github.com/ChimeraCoder/anaconda"
	"github.com/mvdan/xurls"
	"google.golang.org/appengine"
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

	//Get the query string and validate that it is not an error
	query, err := getQuery(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	rawTweets := make(chan anaconda.Tweet)
	shortAddr := make(chan tweetdata.LinkTweet)
	longAddr := make(chan tweetdata.LinkTweet)

	c := appengine.NewContext(request)

	go convertAddresses(shortAddr, longAddr)
	go extractLinks(rawTweets, shortAddr)
	go getTweets(query, c, rawTweets)
	for addresses := range longAddr {
		fmt.Fprintf(writer, "Addresses found: %v", addresses.Address)
	}
}

//getQuery pulls the query from the q paramenter of the query string
func getQuery(request *http.Request) (string, error) {
	result := request.URL.Query().Get(queryParam)

	if result == "" {
		return "", errors.New("No query specified.")
	}
	return result, nil
}

//getTweets gets all tweets from twitter with the speified keyword
func getTweets(query string, c context.Context, out chan<- anaconda.Tweet) {

	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecretKey)

	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)

	api.HttpClient.Transport = &urlfetch.Transport{Context: c}

	result, err := api.GetSearch(query, nil)

	if err != nil {
		log.Printf("Harvester- getTweets: %v", err.Error())
	} else {
		log.Printf("Harvester- getTweets: Retrived %v tweets", len(result.Statuses))
	}

	for _, tweet := range result.Statuses {
		//log.Printf("Tweet: %v", tweet.Text)
		out <- tweet
	}
	close(out)
}

//extractLinks searches the contents of a tweet and pulls out any url from the
// text.  Results in a channel of LinkTweet objects.
func extractLinks(tweets <-chan anaconda.Tweet, out chan<- tweetdata.LinkTweet) {
	for tweet := range tweets {

		rawAddr := xurls.Relaxed.FindString(tweet.Text)

		if rawAddr != "" {
			addr, err := url.Parse(rawAddr)
			if err != nil {
				log.Printf("Harvester- extractLinks: %v", err.Error())
			}

			linkTweet := tweetdata.LinkTweet{
				Address: addr,
				Tweet:   tweet,
			}
			out <- linkTweet
		}
	}
	close(out)
}

//convertAddresses converts shortend addresses to their original format.
func convertAddresses(links <-chan tweetdata.LinkTweet, out chan<- tweetdata.LinkTweet) {

	for tweet := range links {
		response, _ := http.Get(tweet.Address.String())
		tweet.Address = response.Request.URL
	}

	close(out)
}

func writeToDataStore(links <-chan tweetdata.LinkTweet, c context.Context) {
	for link := range links {
		err := tweetdata.WriteLinkTweet(link, c)
		if err != nil {
			log.Printf("Harvester - writeTodataStore: %v", err.Error())
		}
	}
}
