package main

import (
	"errors"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"github.com/ChimeraCoder/anaconda"
)

const linkTweetKind string = "LinkTweet"
const tweetKey string = "Tweets"
const tweetKeyID string = "default_tweetstore"

//LinkTweet contains the address extracted from a tweet and the original tweet
type LinkTweet struct {
	Address string
	Tweet   anaconda.Tweet
}

//WriteLinkTweet writes a given Tweet to the datastore
func WriteLinkTweet(c context.Context, tweets <-chan LinkTweet, semaphore chan<- int) {
	var keys []*datastore.Key
	var values []*StoreTweet

	for tweet := range tweets {
		//log.Infof(c, "Putting Tweet into datastore: %v", tweet.Tweet.Id)

		//log.Infof(c, "Key inputs: Kind - %v \t ParentKey: %v", linkTweetKind, getTweetKey(c).String())

		key := datastore.NewIncompleteKey(c, linkTweetKind, getTweetKey(c))
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

			log.Criticalf(c, "%v", err.Error())
			return
		}
	}
	err := datastore.RunInTransaction(c, func(c context.Context) error {
		_, err := datastore.PutMulti(c, keys, values)

		if err != nil {
			log.Errorf(c, "Failed to write LinkTweet to datastore. %v", err.Error())
			return err
		}
		return nil
	}, nil)
	if err != nil {
		log.Errorf(c, "Failed to write LinkTweet to datastore. %v", err.Error())
	}
	semaphore <- 1
}

//GetAllNewTweets queries the datastore and gets all tweets created since the last
// time given
func GetAllNewTweets(since time.Time, c context.Context) []StoreTweet {
	log.Infof(c, "Getting all tweets newer than: %v", since)
	q := datastore.NewQuery(linkTweetKind).Ancestor(getTweetKey(c)).Filter("CreatedTime >", since)
	out := make([]StoreTweet, 0, 15)
	q.GetAll(c, &out)
	return out
}

func getNewestTweet(c context.Context) time.Time {
	var latest StoreTweet

	//Get just the key for the newest tweet
	q := datastore.NewQuery(linkTweetKind).Order("-CreatedTime").Project("CreatedTime").Limit(1)
	q.GetAll(c, latest)
	i := q.Run(c)
	i.Next(&latest)
	return latest.CreatedTime
}

// guestbookKey returns the key used for all guestbook entries.
func getTweetKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, tweetKey, tweetKeyID, 0, nil)
}
