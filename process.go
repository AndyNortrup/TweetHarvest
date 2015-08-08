package main

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

//TweetScore is a struct that shows the relative score of an address based on
// it's populatrity
type TweetScore struct {
	Address    string
	Score      int
	LastActive time.Time
	TweetIDs   []int64
	Keyword    string
}

//TweetProcessor is an instance of an HTTP server
type TweetProcessor struct {
	c context.Context
}

//Constans are used to standardize strings used for data access from the Datastore.
const tweetScoreKind string = "TweetScore"
const scoreKey string = "Scores"
const scoreKeyID string = "default_scorestore"

//ServeHTTP is an Handler for Process requests.  It serves as the reduce function of
// the system, creating a score, for each of the addresses found in tweets.
func (tp TweetProcessor) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	tp.c = appengine.NewContext(request)
	log.Infof(tp.c, "Starting Reduce Processing.")

	//Wait group keeps the process open until all go routines are complete inc to 1 for
	// the calculatenewScores routines
	var wg sync.WaitGroup
	wg.Add(1)

	//new Scores is a channel that passess TweetScore structs between calculateNewScores
	// and updateDataStoreScore
	newScores := make(chan *TweetScore)
	go tp.calculateNewScores(newScores, &wg)

	//iterate over the contents of the newScores channel and update the datastore
	for score := range newScores {
		wg.Add(1)
		go tp.updateDataStoreScore(*score, &wg)
	}

	//Hold until updateDatastore is compelete
	wg.Wait()
	writer.WriteHeader(http.StatusOK)
}

func (tp TweetProcessor) calculateNewScores(out chan<- *TweetScore, wg *sync.WaitGroup) {
	log.Infof(tp.c, "Calculating New Scores")

	//Tell the wait group that the method is done when it completes.
	defer wg.Done()

	//Score map holds a mapping of addresses to their scores.
	score := make(map[string]*TweetScore)

	//Get all new tweets since the last time the reduce process was run than range over
	// those tweets.
	tweets := GetAllNewTweets(tp.getLastProcessedTweet(), tp.c)
	for _, data := range tweets {

		//If the map does not contain a key for this address, create a new value and add
		// it to the map.
		if score[data.Address] == nil {
			score[data.Address] = &TweetScore{Address: data.Address}
		}

		//Update the score, LastAvtive, and TweetIDs data with the current tweet
		score[data.Address].Score = score[data.Address].Score + data.getScore()
		score[data.Address].LastActive = data.CreatedTime
		score[data.Address].TweetIDs = append(score[data.Address].TweetIDs, data.TweetID)
	}

	//Range over the map and output the values into the channel for further processing
	for _, data := range score {
		log.Infof(tp.c, "Calculate: Address: %v\tScore: %v", data.Address, data.Score)
		out <- data
	}
	//Close the channel to indicate that there is no more data
	close(out)
}

//updateDataStoreScore is a method that updates or creates records in the datastore with
// with the contents of the a TweetScore struct
func (tp TweetProcessor) updateDataStoreScore(score TweetScore, wg *sync.WaitGroup) {
	//At the end of the method close the score
	defer wg.Done()

	//Get the existing score for this address from the datastore.
	//log.Infof(tp.c, "Pulling existing score for address: %v", score.Address)
	q := datastore.NewQuery(tweetScoreKind).
		Filter("Address =", score.Address).
		Ancestor(getTweetScoreKey(tp.c))

	iterator := q.Run(tp.c)

	oldScore := &TweetScore{}
	key, err := iterator.Next(oldScore)
	if err != nil && err == datastore.Done {
		//log.Infof(tp.c, "No old score exists for this address.")
		//No old score exists, so we just add the new one
		oldScore.Address = score.Address
		oldScore.LastActive = score.LastActive
		oldScore.Score = score.Score
		oldScore.TweetIDs = score.TweetIDs

		//Create a new key
		key = datastore.NewIncompleteKey(tp.c, tweetScoreKind, getTweetScoreKey(tp.c))
	} else {
		//We have an old score, increment the score, add new TweetIDs, and update LastActive
		//log.Infof(tp.c, "Old score for this address is: %v", oldScore.Score)
		oldScore = &TweetScore{Score: score.Score,
			LastActive: score.LastActive,
			TweetIDs:   score.TweetIDs,
		}
	}

	//log.Infof(tp.c, "Writing to database: %v\t%v", oldScore.Address, oldScore.Score)
	datastore.Put(tp.c, key, oldScore)
}

//Gets the last date that any score was active, which should be the date of the last processed tweet
func (tp TweetProcessor) getLastProcessedTweet() time.Time {
	//Get a single value from the datastore with the newest date
	q := datastore.NewQuery(tweetScoreKind).Project("LastActive").Order("-LastActive").Limit(1)
	i := q.Run(tp.c)

	score := &TweetScore{}
	_, err := i.Next(score)
	if err != nil {
		//If there is no date (I.E. this is the first time we have run this process) return
		// a date of one day prior, to keep processing size under control.
		log.Infof(tp.c, "Could not find last processed tweet date, returning one day ago")
		return time.Now().AddDate(0, 0, -1)
	}
	//log.Infof(tp.c, "Last tweet scored is dated: %v", score.LastActive)
	return score.LastActive
}

//getTweetScoreKey returns the same key every time so that all TweetScore entites have
// a common Ancestor
func getTweetScoreKey(c context.Context) *datastore.Key {
	// The string "default_guestbook" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, scoreKey, scoreKeyID, 0, nil)
}
