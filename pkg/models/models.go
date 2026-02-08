package models

type Document map[string]interface{}

type SearchRequest struct {
	Query string `json:"query"`
	From  int    `json:"from"`
	Size  int    `json:"size"`
}

type SearchResponse struct {
	Total    uint64                   `json:"total"`
	Hits     []Hit                    `json:"hits"`
	Took     int64                    `json:"took"`
}

type Hit struct {
	ID     string                 `json:"id"`
	Score  float64                `json:"score"`
	Source map[string]interface{} `json:"source"`
}
