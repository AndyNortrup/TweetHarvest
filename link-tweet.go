package main

import (
	"reflect"

	"google.golang.org/appengine/datastore"

	"github.com/AndyNortrup/anaconda"
)

//LinkTweet contains the address extracted from a tweet and the original tweet
type LinkTweet struct {
	Address string
	anaconda.Tweet
	Query string
}

//LinkTweets is a sortable collection of LinkTweet structs
type LinkTweets []*LinkTweet

const linkTweetKind string = "LinkTweet"

//LinkTweetFrom creates a LinkTweet by extracting an address from the given
//Tweet.  If no link is found then an error is returned
func LinkTweetFrom(tweet anaconda.Tweet) (LinkTweet, error) {

	//log.Infof(c, "Extracted Link: %v", rawAddr)
	linkTweet := LinkTweet{
		Address: tweet.Entities.Urls[0].Expanded_url,
		Tweet:   tweet,
	}
	return linkTweet, nil
}

func (linkTweet LinkTweet) getScore() int {
	return linkTweet.FavoriteCount + 1
}

//Load fulfills the PropertyLoadSaver interface
func (linkTweet *LinkTweet) Load(in <-chan datastore.Property) error {
	propertyList := []datastore.Property{}
	for x := range in {
		propertyList = append(propertyList, x)
	}
	if err := datastore.LoadStruct(linkTweet, propertyList); err != nil {
		return err
	}
	return nil
}

//Save fullfills the PropertyLoadSaver interface
func (linkTweet *LinkTweet) Save(out chan<- datastore.Property) error {
	st := reflect.TypeOf(linkTweet)
	for x := 0; x < st.NumField(); x++ {
		//Hashtags is a slice, inside a slice, and we we don't need it at the moment
		if !st.Field(x).Anonymous && st.Field(x).Name == "Entities" {
			out <- datastore.Property{
				Name:  st.Field(x).Name,
				Value: reflect.ValueOf(st.Field(x)),
			}
		}
	}

	return nil
}

//Len returns the length of the collection
func (s LinkTweets) Len() int {
	return len(s)
}

//Less compares two items in the slice based on the Tweet ID
func (s LinkTweets) Less(i, j int) bool {
	return s[i].Id < s[i].Id
}

//Swap changes the position of two items in the collection
func (s LinkTweets) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
