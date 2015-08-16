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
	User        string
}

//StoreTweets is a sortable list of StoreTweet
type StoreTweets []*StoreTweet

func (st StoreTweet) getScore() int {
	return st.Favorites + 1
}

//Len returns the length of the collection
func (s StoreTweets) Len() int {
	return len(s)
}

//Less compares two items in the slice baed on the tweetScore.Score
func (s StoreTweets) Less(i, j int) bool {
	return s[i].TweetID < s[i].TweetID
}

func (s StoreTweets) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
