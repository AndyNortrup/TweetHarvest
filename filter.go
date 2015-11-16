package tweetharvest

import "github.com/ChimeraCoder/anaconda"

//Filter defines an interface that can be used to filter a tweet.
type Filter interface {
	Filter(tweet *anaconda.Tweet) bool
}

//FilterTweet, takes a single tweet, passes it through the specified Filter type
// and if it passes places it in the outgoing  channel
func FilterTweet(tweet *anaconda.Tweet, filterer Filter, out chan<- *anaconda.Tweet) {
	if filterer.Filter(tweet) {
		out <- tweet
	}
}
