package main

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
	"tweetdata"

	"google.golang.org/appengine"
)

//TweetProcessor is an instance of an HTTP server
type TweetProcessor struct{}

var score map[url.URL]int

func init() {

}

func (tp TweetProcessor) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	c := appengine.NewContext(request)
	tweets := tweetdata.GetAllNewTweets(time.Now().AddDate(0, 0, -1), c)
	for _, data := range tweets {
		fmt.Fprintf(writer, "Address: %v", data.Address)
	}
}
