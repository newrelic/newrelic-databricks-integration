package spark

import (
	"context"
	"encoding/json"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

type SparkApiClient struct {
	sparkContextUiUrl		string
	authenticator			connectors.HttpAuthenticator
}

func NewSparkApiClient(
	sparkContextUiUrl string,
	authenticator connectors.HttpAuthenticator,
) *SparkApiClient {
	return &SparkApiClient{
		sparkContextUiUrl,
		authenticator,
	}
}

func (s *SparkApiClient) GetApplications(
	ctx context.Context,
) ([]SparkApplication, error) {
	sparkApps := []SparkApplication{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications",
		s.authenticator,
		&sparkApps,
	)
	if err !=  nil {
		return nil, err
	}

	return sparkApps, nil
}

func (s *SparkApiClient) GetApplicationExecutors(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkExecutor, error) {
	executors := []SparkExecutor{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/executors",
		s.authenticator,
		&executors,
	)
	if err !=  nil {
		return nil, err
	}

	return executors, nil
}

func (s *SparkApiClient) GetApplicationJobs(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkJob, error) {
	jobs := []SparkJob{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/jobs",
		s.authenticator,
		&jobs,
	)
	if err !=  nil {
		return nil, err
	}

	return jobs, nil
}

func (s *SparkApiClient) GetApplicationStages(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkStage, error) {
	stages := []SparkStage{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/stages?details=true",
		s.authenticator,
		&stages,
	)
	if err != nil {
		return nil, err
	}

	return stages, nil
}

func (s *SparkApiClient) GetApplicationRDDs(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkRDD, error) {
	rdds := []SparkRDD{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/storage/rdd",
		s.authenticator,
		&rdds,
	)
	if err != nil {
		return nil, err
	}

	return rdds, nil
}

func (s *SparkApiClient) GetApplicationStreamingStatistics(
	ctx context.Context,
	app *SparkApplication,
) (*SparkStreamingStatistics, error) {
	stats := SparkStreamingStatistics{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/streaming/statistics",
		s.authenticator,
		&stats,
	)
	if err != nil {
		/*
		// This depends on a change to the SDK that is not yet release.
		httpErr, ok := err.(connectors.HttpError)
		if !ok {
			return nil, err
		}

		if httpErr.StatusCode == 404 {
			// 404 _should_ mean there are no streaming listeners attached to
			// the app. I am not sure if 404 is returned in other circumstances
			// but for now assume not and return no response but also no error.
			log.Debugf(
				"received 404 from streaming statistics API for application %s; assuming app has no streaming listeners and ignoring",
				app.Id,
			)

			return nil, nil
		}
		*/

		return nil, err
	}

	return &stats, nil
}

func (s *SparkApiClient) GetApplicationStreamingReceivers(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkStreamingReceiver, error) {
	receivers := []SparkStreamingReceiver{}

	err := makeRequest(
		s.sparkContextUiUrl + "/api/v1/applications/" + app.Id + "/streaming/receivers",
		s.authenticator,
		&receivers,
	)
	if err != nil {
		/*
		// This depends on a change to the SDK that is not yet release.
		httpErr, ok := err.(connectors.HttpError)
		if !ok {
			return nil, err
		}

		if httpErr.StatusCode == 404 {
			// 404 _should_ mean there are no streaming listeners attached to
			// the app. I am not sure if 404 is returned in other circumstances
			// but for now assume not and return no response but also no error.
			log.Debugf(
				"received 404 from streaming receivers API for application %s; assuming app has no streaming listeners and ignoring",
				app.Id,
			)

			return nil, nil
		}
		*/

		return nil, err
	}

	return receivers, nil
}

func makeRequest(
	url string,
	authenticator connectors.HttpAuthenticator,
	response interface{},
) error {
	connector := connectors.NewHttpGetConnector(url)

	if authenticator != nil {
		connector.SetAuthenticator(authenticator)
	}

	connector.SetHeaders(map[string]string {
		"Content-Type": "application/json",
		"Accept": "application/json",
		"User-Agent": connectors.GetUserAgent(),
	})

	in, err := connector.Request()
	if err != nil {
		return err
	}

	defer in.Close()

	log.Debugf("decoding spark JSON response for URL %s", url)

	dec := json.NewDecoder(in)

	err = dec.Decode(response)
	if err != nil {
		return err
	}

	if log.IsDebugEnabled() {
		log.PrettyPrintJson(response)
	}

	return nil
}
