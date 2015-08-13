package main

import (
	"errors"

	"github.com/ChimeraCoder/anaconda"
	"github.com/mvdan/xurls"
)

//LinkTweet contains the address extracted from a tweet and the original tweet
type LinkTweet struct {
	Address string
	Tweet   anaconda.Tweet
	Query   string
	Title   string
}

//LinkTweetFrom creates a LinkTweet by extracting an address from the given
//Tweet.  If no link is found then an error is returned
func LinkTweetFrom(tweet anaconda.Tweet) (LinkTweet, error) {
	rawAddr := xurls.Strict.FindString(tweet.Text)

	if rawAddr == "" {
		return LinkTweet{}, errors.New("No link found.")
	}
	//log.Infof(c, "Extracted Link: %v", rawAddr)
	linkTweet := LinkTweet{
		Address: rawAddr,
		Tweet:   tweet,
	}
	return linkTweet, nil
}
