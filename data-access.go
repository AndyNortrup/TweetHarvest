package tweetharvest

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const tweetKey string = "Tweets"
const tweetKeyID string = "default_tweetstore"

//GetAllNewTweets queries the datastore and gets all tweets created since the last
// time given
func GetAllNewTweets(since time.Time, c context.Context) LinkTweets {
	log.Infof(c, "Getting all tweets newer than: %v", since)
	q := datastore.NewQuery(linkTweetKind).Ancestor(getTweetKey(c)).Filter("CreatedTime >", since)
	out := make(LinkTweets, 0, 15)
	q.GetAll(c, &out)
	return out
}

func getNewestTweet(c context.Context) time.Time {
	var latest LinkTweet

	//Get just the key for the newest tweet
	q := datastore.NewQuery(linkTweetKind).Order("-CreatedTime").Project("CreatedTime").Limit(1)
	q.GetAll(c, latest)
	i := q.Run(c)
	i.Next(&latest)
	time, _ := latest.CreatedAtTime()
	return time
}

// guestbookKey returns the key used for all guestbook entries.
func getTweetKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, tweetKey, tweetKeyID, 0, nil)
}

//LinkTweetFromDatastore returns a LinkTweet from the DataStore that has the given TweetID
func LinkTweetFromDatastore(tweetID int64, c context.Context) *LinkTweet {
	q := datastore.NewQuery(linkTweetKind).
		Filter("TweetID =", tweetID).
		Limit(1)

	iterator := q.Run(c)
	linkTweet := &LinkTweet{}
	_, err := iterator.Next(linkTweet)
	if err != nil {
		return nil
	}
	return linkTweet
}
