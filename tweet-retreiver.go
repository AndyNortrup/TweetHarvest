package main

import (
	"sync"
	"time"

	"github.com/ChimeraCoder/anaconda"
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

//TweetRetriever is responsible for getting a list of Tweets from the Twitter API
type TweetRetriever struct {
	context context.Context
}

//getTweets gets all tweets from twitter with the speified keyword
func (tr TweetRetriever) getTweets(query string,
	cutoff time.Time,
	out chan anaconda.Tweet,
	wg *sync.WaitGroup) {

	defer wg.Done()

	log.Infof(tr.context, "Downloading Tweets.")
	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecretKey)

	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)

	api.HttpClient.Transport = &urlfetch.Transport{Context: tr.context}

	result, err := api.GetSearch(query, nil)

	if err != nil {
		log.Errorf(tr.context, "Harvester- getTweets: %v", err.Error())
		return
	}
	cont := true

	for cont {
		cont = tr.addIfNewerThan(cutoff, result, out)
		cont = false
		if cont {
			result, err = result.GetNext(api)
			//log.Infof(c, "Getting more tweets!")
			if err != nil {
				log.Errorf(tr.context, "Harvester- getTweets: %v", err.Error())
			}
		}
	}
	close(out)
}

func (tr TweetRetriever) addIfNewerThan(cutoff time.Time,
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
