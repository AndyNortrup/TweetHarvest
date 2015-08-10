package main

import (
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
	Query   string
	Title   string
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
