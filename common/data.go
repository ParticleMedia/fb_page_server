package common

type FBProfile struct {
	Id    int32    `json:"_id"`
	Pages []FBPage `json:"likes"`
}

type FBPage struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	About    string `json:"about"`
	Category string `json:"category"`
}