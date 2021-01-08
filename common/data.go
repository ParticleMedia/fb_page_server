package common

var sourceMap map[string]*SourceInfo

func GetSourceInfo(domain string) *SourceInfo {
	if len(domain) > 0 {
		return sourceMap[domain]
	}
	return nil
}
