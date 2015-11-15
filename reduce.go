package tweetharvest

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/html"

	"github.com/ChimeraCoder/anaconda"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

//Reducer is an instance of an HTTP server
type Reducer struct {
	c context.Context
}

//Constans are used to standardize strings used for data access from the Datastore.
const tweetScoreKind string = "TweetScore"
const scoreKey string = "Scores"
const scoreKeyID string = "default_scorestore"

//ServeHTTP is an Handler for Process requests.  It serves as the reduce function of
// the system, creating a score, for each of the addresses found in tweets.
func (reduce Reducer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	reduce.c = appengine.NewContext(request)
	log.Infof(reduce.c, "Starting Reduce Processing.")

	//Wait group keeps the process open until all go routines are complete inc to 1 for
	// the calculatenewScores routines
	var wg sync.WaitGroup
	wg.Add(1)

	//tweets is a channel for holding TweetIDs
	tweets := make(chan *anaconda.Tweet)
	go reduce.queryForTweets(tweets, &wg)

	//Hold until updateDatastore is compelete
	wg.Wait()
	writer.WriteHeader(http.StatusOK)
}

func (reduce Reducer) queryForTweets(out chan<- *anaconda.Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	//lastProcess := reduce.getLastProcessedTweet()

}
func (reduce Reducer) calculateNewScores(out chan<- *TweetScore, wg *sync.WaitGroup) {
	log.Infof(reduce.c, "Calculating New Scores")

	//Tell the wait group that the method is done when it completes.
	defer wg.Done()

	//Score map holds a mapping of addresses to their scores.
	score := make(map[string]*TweetScore)

	//Get all new tweets since the last time the reduce process was run than range over
	// those tweets.
	tweets := GetAllNewTweets(reduce.getLastProcessedTweet(), reduce.c)
	for _, data := range tweets {

		//If the map does not contain a key for this address, create a new value and add
		// it to the map.
		if score[data.Address] == nil {
			score[data.Address] = &TweetScore{
				Address: data.Address,
				Query:   data.Query,
			}
		}

		//Update the score, LastAvtive, and TweetIDs data with the current tweet
		score[data.Address].Score = score[data.Address].Score + data.getScore()
		score[data.Address].LastActive, _ = data.CreatedAtTime()
		score[data.Address].TweetIDs = append(score[data.Address].TweetIDs, data.Id)
	}

	//Range over the map and output the values into the channel for further processing
	for _, data := range score {
		log.Infof(reduce.c, "Calculate: Address: %v\tScore: %v", data.Address, data.Score)
		out <- data
	}
	//Close the channel to indicate that there is no more data
	close(out)
}

//updateDataStoreScore is a method that updates or creates records in the datastore with
// with the contents of the a TweetScore struct
func (reduce Reducer) updateDataStoreScore(score TweetScore, wg *sync.WaitGroup) {
	//At the end of the method close the score
	defer wg.Done()

	//Get the existing score for this address from the datastore.
	//log.Infof(reduce.c, "Pulling existing score for address: %v", score.Address)
	q := datastore.NewQuery(tweetScoreKind).
		Filter("Address =", score.Address).
		Ancestor(getTweetScoreKey(reduce.c))

	iterator := q.Run(reduce.c)

	oldScore := &TweetScore{}
	key, err := iterator.Next(oldScore)
	if err != nil && err == datastore.Done {
		//log.Infof(reduce.c, "No old score exists for this address.")
		//No old score exists, so we just add the new one

		oldScore.Title, err = reduce.
			getTitle(score.Address)
		if err != nil {
			log.Infof(reduce.c, "Failed to GET address: %v \n\t%v", score.Address, err.Error())
		}
		oldScore.Address = score.Address
		oldScore.LastActive = score.LastActive
		oldScore.Score = score.Score
		oldScore.TweetIDs = score.TweetIDs
		oldScore.Query = score.Query

		//Create a new key
		key = datastore.NewIncompleteKey(reduce.c, tweetScoreKind, getTweetScoreKey(reduce.c))
	} else {
		//We have an old score, increment the score, add new TweetIDs, and update LastActive
		//log.Infof(reduce.c, "Old score for this address is: %v", oldScore.Score)
		oldScore = &TweetScore{Score: score.Score,
			LastActive: score.LastActive,
			TweetIDs:   score.TweetIDs,
		}
	}

	//log.Infof(reduce.c, "Writing to database: %v\t%v", oldScore.Address, oldScore.Score)
	datastore.Put(reduce.c, key, oldScore)
}

//getTitle retrives the content of an address then sends the body to the scraper
// to in order to find the title which is returnted to the user.
func (reduce Reducer) getTitle(address string) (string, error) {
	client := urlfetch.Client(reduce.c)
	resp, err := client.Get(address)

	if err != nil {
		return "", err
	}
	return reduce.scrapeTitle(resp)
}

//scrapeTitle parses through an HTML document to find the <title> tag then get
// the content of that tag and return it to the user.
func (reduce Reducer) scrapeTitle(resp *http.Response) (string, error) {
	defer resp.Body.Close()

	if strings.ToLower(resp.Header.Get("Content-Type")) != "text/html; charset=utf-8" {
		message := "Wrong content type.  Recieved: " + resp.Header.Get("Content-Type") + " from " + resp.Request.URL.String()
		log.Infof(reduce.c, message)
		return "", errors.New(message)
	}

	tokenizer := html.NewTokenizer(resp.Body)
	var titleIsNext bool
	for {
		token := tokenizer.Next()
		switch {
		case token == html.ErrorToken:
			log.Infof(reduce.c, "Hit the end of the doc without finding title.")
			return "", errors.New("Unable to find title tag in " + resp.Request.URL.String())
		case token == html.StartTagToken:
			tag := tokenizer.Token()
			isTitle := tag.Data == "title"

			if isTitle {
				titleIsNext = true
			}
		case titleIsNext && token == html.TextToken:
			title := tokenizer.Token().Data
			log.Infof(reduce.c, "Pulled title: %v", title)
			return title, nil
		}
	}
}

//getLastProcessedTweet gets the last date that any score was active,
// which should be the date of the last processed tweet
func (reduce Reducer) getLastProcessedTweet() time.Time {
	//Get a single value from the datastore with the newest date
	q := datastore.NewQuery(tweetScoreKind).
		Project("LastActive").
		Order("-LastActive").Limit(1)

	i := q.Run(reduce.c)

	score := &TweetScore{}
	_, err := i.Next(score)
	if err != nil {
		//If there is no date (I.E. this is the first time we have run this process) return
		// a date of one day prior, to keep processing size under control.
		log.Infof(reduce.c, "Could not find last processed tweet date, returning one day ago")
		return time.Now().AddDate(0, 0, -1)
	}
	//log.Infof(reduce.c, "Last tweet scored is dated: %v", score.LastActive)
	return score.LastActive
}

//getTweetScoreKey returns the same key every time so that all TweetScore entites have
// a common Ancestor
func getTweetScoreKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, scoreKey, scoreKeyID, 0, nil)
}
