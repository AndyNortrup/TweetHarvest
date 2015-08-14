package main

import (
	"time"

	"golang.org/x/net/context"

	"github.com/gorilla/feeds"
)

//TweetScore is a struct that shows the relative score of an address based on
// it's populatrity
type TweetScore struct {
	Address    string
	Score      int
	LastActive time.Time
	TweetIDs   []int64
	Query      string
	Title      string
}

//GetFeedItem returns a feeds.Item to be inserted into an RSS or Atom feed
func (score TweetScore) GetFeedItem(id string, c context.Context) *feeds.Item {
	entry := &feeds.Item{
		Link:    &feeds.Link{Href: score.Address},
		Created: score.LastActive,
		Id:      id,
	}

	return entry
}

func (score TweetScore) buildDescription(c context.Context) {
	/*
		var wg sync.WaitGroup
		for tweetID := range score.TweetIDs {
			wg.Add(1)
			//Todo: Go routine to get the name of user and content of
			//tweets and add add them to the description.
		}
	*/
}
