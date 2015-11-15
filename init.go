package tweetharvest

import (
	"net/http"

	"github.com/gorilla/mux"
)

func init() {
	th := &MapBuilder{}
	proc := &Reducer{}
	consume := &FeedProducer{}

	plex := mux.NewRouter()
	plex.Handle("/map", th)
	plex.Handle("/reduce", proc)
	plex.Handle("/consume", consume)

	http.Handle("/", plex)

}
