package security

import (
	"sort"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestGetDestinationTypes(t *testing.T) {
	testData := []struct {
		metaData *common.MetaData
		expected []string
	}{
		{&common.MetaData{DestType: "qwer", DestinationsList: []string{}}, []string{"qwer"}},
		{&common.MetaData{DestType: "", DestinationsList: []string{"qwer:tyuiop"}}, []string{"qwer"}},
		{&common.MetaData{DestType: "", DestinationsList: []string{"qwer:tyuio", "asdf:ghjkl", "asdf:zxcv"}}, []string{"qwer", "asdf"}},
	}

	for _, test := range testData {
		result := getDestinationTypes(test.metaData)
		if len(result) == len(test.expected) {
			sort.Strings(result)
			sort.Strings(test.expected)

			for index, objectType := range result {
				if objectType != test.expected[index] {
					t.Errorf("getDestinationTypes returned a type of %s instead of the expected type %s", objectType, test.expected[index])
				}
			}
		} else {
			t.Errorf("getDestinationTypes returned %d types instead of the expected %d types", len(result), len(test.expected))
		}
	}
}
