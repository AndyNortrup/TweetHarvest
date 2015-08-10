package main

import (
	"time"
)

//StoreTweet is a struct used for storing a tweet in the datastore
type StoreTweet struct {
	Address     string
	Text        string
	TweetID     int64
	CreatedTime time.Time
	Retweets    int
	Favorites   int
	Query       string
	Title       string
}

func (st StoreTweet) getScore() int {
	return st.Favorites + 1
}
