package common

type FBProfile struct {
	Id    int32    `json:"_id"`
	Pages []FBPage `json:"likes"`
}

type FBPage struct {
	Id       string `json:"_id"`
	Name     string `json:"name"`
	About    string `json:"about"`
	Category string `json:"category"`
}

type PageTcat struct {
	Id   string                        `bson:"_id"`
	Tcat map[string]map[string]float64 `bson:"text_category"`
}

type TextCategoryBody struct {
	Tcats TextCategory `json:"text_category"`
}

type TextCategory struct {
	FirstCats  map[string]float64 `json:"first_cat"`
	SecondCats map[string]float64 `json:"second_cat"`
	ThirdCats  map[string]float64 `json:"third_cat"`
}
