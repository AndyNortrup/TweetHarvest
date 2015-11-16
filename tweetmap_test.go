package tweetharvest

import "testing"

func TestMapAdd(t *testing.T) {
	tweetMap := make(TweetMap)

	tweetMap.Add("test", 1)
	if len(tweetMap["test"]) != 1 {
		t.Errorf("Failed to add a value to the TweetMap")
	}

	tweetMap.Add("test", 2)
	if len(tweetMap["test"]) != 2 {
		t.Errorf("Failed to add a value to the TweetMap")
	}

	tweetMap.Add("url", 3)
	if len(tweetMap["url"]) != 1 {
		t.Errorf("Failed to add a value to the TweetMap")
	}
}
