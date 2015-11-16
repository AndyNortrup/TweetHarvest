package tweetharvest

import "github.com/ChimeraCoder/anaconda"

//URLFilter filters tweets based upon if they have an embeded URL
type URLFilter struct{}

//Filter is an implementation of the Filter interface and returns true if the
//input tweet has a URL embeded
func (filter URLFilter) Filter(tweet *anaconda.Tweet) bool {
	return len(tweet.Entities.Urls) > 0
}
