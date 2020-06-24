package common

import "strings"

const (
	MODE_STRICT = iota
	MODE_RELAX

	SCOPE_GLOBAL = iota
	SCOPE_STATE
	SCOPE_DMA
	SCOPE_COUNTY
	SCOPE_CITY
	SCOPE_ZIP
	SCOPE_NEIGHBORHOOD
)

var GeoTypeToScopeMap = map[string]int{
	"state": SCOPE_STATE,
	"county": SCOPE_COUNTY,
	"city": SCOPE_CITY,
	"poi": SCOPE_NEIGHBORHOOD,
	"area": SCOPE_CITY,
}

func GeoTypeToScope(geoType string) int {
	scope, ok := GeoTypeToScopeMap[geoType]
	if ok {
		return scope
	} else {
		return SCOPE_GLOBAL
	}
}

func ReplaceSpace(raw string) string {
	return strings.ReplaceAll(raw, " ", "^^")
}
