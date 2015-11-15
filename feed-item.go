package tweetharvest

import (
	"bufio"
	"bytes"
	"html/template"
	"sort"
	"sync"

	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"golang.org/x/net/context"

	"github.com/gorilla/feeds"
)

const embed = `<HTML><BODY><UL>
{{range .}}
<LI>{{.Text}}</LI>
{{end}}
</UL></BODY></HTML>`

//FeedItem is a struct that provides a description for a TweetScore
type FeedItem struct {
	description string
	TweetScore
	key string
}

func (item *FeedItem) getItem() *feeds.Item {
	var out feeds.Item

	out.Created = item.LastActive
	out.Title = item.Title
	out.Link = &feeds.Link{Href: item.Address}
	out.Description = item.description

	return &out
}

//BuildDescription creates a feed item by retrieving the twitter embed code from
// twitter API
func (item *FeedItem) BuildDescription(c context.Context) {
	var embedWG sync.WaitGroup

	embeds := make(chan *LinkTweet)
	for _, tweetID := range item.TweetIDs {
		embedWG.Add(1)
		go item.getEmbedFor(tweetID, embeds, &embedWG, c)
	}

	chanDesc := make(chan string, 1)
	go item.combineDescription(embeds, chanDesc, c)

	embedWG.Wait()
	close(embeds)

	item.description = <-chanDesc
}

//Send a request to Twitter for the embed.
func (item *FeedItem) getEmbedFor(tweetID int64,
	out chan<- *LinkTweet,
	wg *sync.WaitGroup,
	c context.Context) {

	defer wg.Done()
	iterator := datastore.NewQuery(linkTweetKind).
		Project("User", "Text").
		Filter("TweetID=", tweetID).
		Limit(1).
		Run(c)

	tweet := &LinkTweet{}
	_, err := iterator.Next(tweet)
	if err != nil {
		log.Errorf(c, "Error getting tweets for embed. \n\t%v", err.Error())
	}
	out <- tweet
}

//buildDescription compiles the individual html parts from the getEmbedFor
// routines
func (item *FeedItem) combineDescription(
	in <-chan *LinkTweet,
	out chan<- string,
	c context.Context) {

	var parts LinkTweets
	for tweet := range in {
		parts = append(parts, tweet)
	}
	sort.Sort(parts)

	var tweetTemplate = template.Must(template.New("tweet").Parse(embed))
	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	err := tweetTemplate.Execute(w, parts)
	if err != nil {
		log.Errorf(c, "Error writing template.  \n\tStoreTweets: %v\n\t%v", parts, err.Error)
	}

	w.Flush()

	log.Infof(c, "Description: %v", b.String())
	out <- string(b.String())

	close(out)
}
