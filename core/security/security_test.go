package security

import (
	"sort"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

// TestGetDestinationTypes tests extraction of destination types from metadata:
// - Single destination type from DestType field
// - Single destination type from DestinationsList
// - Multiple destination types from DestinationsList with duplicates
//
// This ensures that the getDestinationTypes helper function correctly extracts
// unique destination types from both the DestType field and DestinationsList,
// which is critical for ACL validation and authorization checks. The function
// must handle duplicates and return a deduplicated list of destination types.
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
