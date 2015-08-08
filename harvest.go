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
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

//TweetHarvester is a microservice that querries the Twitter API and gets copies
// of all of the tweets with a specified string from the query string.
type TweetHarvester struct {
	c context.Context
}

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
	th.c = appengine.NewContext(request)
	log.Infof(th.c, "Starting Tweet Harvest.")

	//Get the query string and validate that it is not an error
	query, err := th.getQuery(request)
	if err != nil {
		log.Errorf(th.c, "Failed to get query from querystring.")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	log.Infof(th.c, "Newest Tweet in datastore is dated: %v", getNewestTweet(th.c).String())

	rawTweets := make(chan anaconda.Tweet)
	shortLinkTweets := make(chan LinkTweet)
	longLinkTweets := make(chan LinkTweet, 15)

	var wg sync.WaitGroup
	wg.Add(4)
	go th.getTweets(query, rawTweets, &wg)
	go th.extractLinks(rawTweets, shortLinkTweets, &wg)
	go th.convertAddresses(shortLinkTweets, longLinkTweets, &wg)
	go th.WriteLinkTweet(longLinkTweets, &wg)

	wg.Wait()
	writer.WriteHeader(http.StatusOK)
}

//getQuery pulls the query from the q paramenter of the query string
func (th TweetHarvester) getQuery(request *http.Request) (string, error) {
	result := request.URL.Query().Get(queryParam)

	if result == "" {
		return "", errors.New("No query specified.")
	}
	log.Infof(th.c, "Recived query parameter: %v", result)
	return result, nil
}

//getTweets gets all tweets from twitter with the speified keyword
func (th TweetHarvester) getTweets(query string,
	out chan anaconda.Tweet,
	wg *sync.WaitGroup) {

	defer wg.Done()

	log.Infof(th.c, "Downloading Tweets.")
	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecretKey)

	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)

	api.HttpClient.Transport = &urlfetch.Transport{Context: th.c}

	result, err := api.GetSearch(query, nil)

	if err != nil {
		log.Errorf(th.c, "Harvester- getTweets: %v", err.Error())
		return
	}
	cont := true

	for cont {
		cont = addIfNewerThan(getNewestTweet(th.c), result, out)
		cont = false
		if cont {
			result, err = result.GetNext(api)
			//log.Infof(c, "Getting more tweets!")
			if err != nil {
				log.Errorf(th.c, "Harvester- getTweets: %v", err.Error())
			}
		}
	}
	close(out)
}

func addIfNewerThan(cutoff time.Time,
	result anaconda.SearchResponse,
	output chan anaconda.Tweet) bool {

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
func (th TweetHarvester) extractLinks(tweets <-chan anaconda.Tweet,
	out chan<- LinkTweet,
	wg *sync.WaitGroup) {

	defer wg.Done()

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
func (th TweetHarvester) convertAddresses(links <-chan LinkTweet,
	out chan<- LinkTweet,
	outterWG *sync.WaitGroup) {

	defer outterWG.Done()
	client := urlfetch.Client(th.c)

	var wg sync.WaitGroup

	for tweet := range links {
		wg.Add(1)
		go func(tweet LinkTweet, out chan<- LinkTweet) {

			defer wg.Done()

			response, err := client.Head(tweet.Address)
			if err != nil {
				log.Infof(th.c, "Harvester - convertAddress error: %v", err.Error())
				return
			}
			tweet.Address = response.Request.URL.String()
			out <- tweet
		}(tweet, out)

	}

	wg.Wait()
	close(out)
}

//WriteLinkTweet writes a given Tweet to the datastore
func (th TweetHarvester) WriteLinkTweet(tweets <-chan LinkTweet, wg *sync.WaitGroup) {
	defer wg.Done()

	var keys []*datastore.Key
	var values []*StoreTweet

	for tweet := range tweets {
		//log.Infof(c, "Putting Tweet into datastore: %v", tweet.Tweet.Id)

		//log.Infof(c, "Key inputs: Kind - %v \t ParentKey: %v", linkTweetKind, getTweetKey(c).String())

		key := datastore.NewIncompleteKey(th.c, linkTweetKind, getTweetKey(th.c))
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
		}
		keys = append(keys, key)
		values = append(values, store)

		if key == nil {
			err := errors.New("Key is nil befor put.")

			log.Criticalf(th.c, "%v", err.Error())
			return
		}
	}
	err := datastore.RunInTransaction(th.c, func(c context.Context) error {
		_, err := datastore.PutMulti(c, keys, values)

		if err != nil {
			log.Errorf(c, "Failed to write LinkTweet to datastore. %v", err.Error())
			return err
		}
		return nil
	}, nil)
	if err != nil {
		log.Errorf(th.c, "Failed to write LinkTweet to datastore. %v", err.Error())
	}
}
