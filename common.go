package tweetharvest

import (
	"errors"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

const queryParam string = "q"

//getQuery pulls the query from the q paramenter of the query string
func getQuery(request *http.Request, c context.Context) (string, error) {
	result := request.URL.Query().Get(queryParam)

	if result == "" {
		return "", errors.New("No query specified.")
	}
	log.Infof(c, "Recived query parameter: %v", result)
	return result, nil
}
