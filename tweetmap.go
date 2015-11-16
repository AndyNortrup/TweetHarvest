package tweetharvest

//TweetMap is a map of web addresses mapped to Tweets that mentioned it.
type TweetMap map[string][]int64

//Add safely adds the a URL and TweetID combo to the map
func (tweetMap TweetMap) Add(address string, tweetID int64) {
	if len(tweetMap[address]) > 0 {
		tweetMap[address] = append(tweetMap[address], tweetID)
		return
	}
	tweetMap[address] = []int64{tweetID}
}
