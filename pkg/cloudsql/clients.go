package cloudsql

import (
	"context"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/sqladmin/v1"
)

type CloudMonitoringClient struct {
	client *monitoring.MetricClient
	ctx    context.Context
}

type SqlAdminClient struct {
	client *sqladmin.Service
}

func NewSqlAdminClient(client *sqladmin.Service) SqlAdminClient {
	return SqlAdminClient{
		client: client,
	}
}

func (client *SqlAdminClient) ApplyFlags(projectId string, databaseName string, newFlags []*sqladmin.DatabaseFlags) error {
	// get current flag values (so we don't unset any that are already set!)
	inst, err := client.client.Instances.Get(projectId, databaseName).Do()
	if err != nil {
		return fmt.Errorf("Failed to get instance settings! %v", err)
	}

	// convert those into a map for easier merging
	flagValues := make(map[string]string, len(inst.Settings.DatabaseFlags))

	for _, flag := range inst.Settings.DatabaseFlags {
		fmt.Printf("Previously set database flag, %s: %s\n", flag.Name, flag.Value)
		flagValues[flag.Name] = flag.Value
	}

	// merge in new flags
	for _, flag := range newFlags {
		fmt.Printf("New database flag, %s: %s\n", flag.Name, flag.Value)
		flagValues[flag.Name] = flag.Value
	}

	// convert back to right type
	flags := []*sqladmin.DatabaseFlags{}
	for key, value := range flagValues {
		flags = append(flags, &sqladmin.DatabaseFlags{Name: key, Value: value})
	}

	settings := &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{DatabaseFlags: flags},
	}

	err = applyPatch(client.client, projectId, databaseName, settings)
	if err != nil {
		return err
	}
	// get the latest op which will be the update op caused by above patch
	ops, err := client.client.Operations.List(projectId).Do(googleapi.QueryParameter("instance", databaseName), googleapi.QueryParameter("maxResults", "5"))
	if err != nil {
		return err
	}
	updateOp := ops.Items[0]
	return client.waitForUpdate(projectId, updateOp.Name)
}

func (client *SqlAdminClient) waitForUpdate(projectId string, updateOpId string) error {
	// updates should not take particularly long, so set a window of 2 minutes
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			// we hit the timeout
			return fmt.Errorf("Timeout waiting for config to apply")
		case <-time.After(1 * time.Second):
			// check update operation and see if it is done
			status, err := getOperationStatus(client.client, projectId, updateOpId)
			if err != nil {
				return err
			}
			if status == "DONE" {
				return nil
			}
		}
	}
}

func getOperationStatus(service *sqladmin.Service, projectId string, updateOpId string) (string, error) {
	op, err := service.Operations.Get(projectId, updateOpId).Do()
	if err != nil {
		return "", err
	}

	return op.Status, nil
}
func applyPatch(service *sqladmin.Service, projectId string, databaseName string, database *sqladmin.DatabaseInstance) error {
	patchSuccessful := false

	for !patchSuccessful {
		_, err := service.Instances.Patch(projectId, databaseName, database).Do()
		if e, ok := err.(*googleapi.Error); ok {
			if e.Errors[0].Reason == "operationInProgress" {
				fmt.Printf("Failed to patch %s as operation in progress. Waiting 1 second...\n", databaseName)
				time.Sleep(1 * time.Second)
			} else {
				return fmt.Errorf("Failed to patch %s with unrecoverable error: %v\n", databaseName, e.Errors[0].Reason)
			}
		} else if err != nil {
			return fmt.Errorf("Failed to patch %s with unrecoverable error: %v\n", databaseName, err)
		} else {
			patchSuccessful = true
		}
	}

	return nil
}
