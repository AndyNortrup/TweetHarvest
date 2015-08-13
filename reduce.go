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

	//new Scores is a channel that passess TweetScore structs between calculateNewScores
	// and updateDataStoreScore
	newScores := make(chan *TweetScore)
	go reduce.calculateNewScores(newScores, &wg)

	//iterate over the contents of the newScores channel and update the datastore
	for score := range newScores {
		wg.Add(1)
		go reduce.updateDataStoreScore(*score, &wg)
	}

	//Hold until updateDatastore is compelete
	wg.Wait()
	writer.WriteHeader(http.StatusOK)
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
		score[data.Address].LastActive = data.CreatedTime
		score[data.Address].TweetIDs = append(score[data.Address].TweetIDs, data.TweetID)
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

//Gets the last date that any score was active, which should be the date of the last processed tweet
func (reduce Reducer) getLastProcessedTweet() time.Time {
	//Get a single value from the datastore with the newest date
	q := datastore.NewQuery(tweetScoreKind).Project("LastActive").Order("-LastActive").Limit(1)
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
