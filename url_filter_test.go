package tweetharvest

import (
	"testing"

	"github.com/ChimeraCoder/anaconda"
)

func TestURLFIlter(t *testing.T) {
	//known quantity tweets from the author's feed
	var noLink int64 = 665323700086923264
	var withLink int64 = 665756769528999936

	out := make(chan *anaconda.Tweet, 2)

	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecretKey)
	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)
	tweetNoLink, err := api.GetTweet(noLink, nil)

	var filter URLFilter

	if err != nil {
		t.Errorf("Unable to retrieve Tweet ID from Twitter.  \nError:%v", err)
	}

	FilterTweet(&tweetNoLink, filter, out)

	tweetWithLink, err := api.GetTweet(withLink, nil)

	if err != nil {
		t.Errorf("Unable to retrieve Tweet ID from Twitter.  \nError:%v", err)
	}
	FilterTweet(&tweetWithLink, filter, out)
	output := <-out

	if output.Id == noLink {
		t.Fatalf("URLFilter allowed a tweet with no URL to pass")
	} else if output.Id != withLink {
		t.Fatalf("URLFilter did not select a tweet with a URL Entity")
	}

}
