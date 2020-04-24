package common

import (
	"encoding/json"
	"testing"
)

func TestCppSerialize(t *testing.T) {
	rawData := []byte(`{"cat_class":["los^^angeles^^california"],"docid":"0NBN3i10","domain":"washingtonpost.com","epoch":1572044700,"geotag":[{"score":0.9805656671524048,"coord":"38.440429,-122.714055","name":"santa rosa","pid":"santa_rosa,california","type":"city"},{"score":0.9356310367584229,"coord":"38.577956,-122.988832","name":"sonoma county","pid":"sonoma_county,california","type":"county"}],"hl_geotag":[{"score":0.9805656671524048,"coord":"38.440429,-122.714055","name":"santa rosa","pid":"santa_rosa,california","type":"residential","zipcodes":["94124"]},{"score":0.9356310367584229,"coord":"38.577956,-122.988832","name":"sonoma county","pid":"sonoma_county,california","type":"neighborhood","zipcodes":["94124"]}],"location_std":{"cities":["los_angeles,california"]},"source":"Washington Post","title":"Power may be cut for millions of Calif. residents this weekend as fire risk increases , utility says"}`)
	var cppDoc CppDocument
	err := json.Unmarshal(rawData, &cppDoc)
	if err != nil {
		t.Fatalf("unmarshal json error: %+v\n", err)
	}

	doc := NewIndexerDocumentFromCpp(&cppDoc)
	t.Logf("%+v", doc)
}
