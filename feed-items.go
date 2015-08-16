package main

//FeedItems is a sortable slice of FeedItem objects.
type FeedItems []*FeedItem

//Len returns the length of the collection
func (s FeedItems) Len() int {
	return len(s)
}

//Less compares two items in the slice baed on the tweetScore.Score
func (s FeedItems) Less(i, j int) bool {
	return s[i].Score < s[i].Score
}

func (s FeedItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
