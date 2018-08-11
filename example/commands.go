package main

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value interface{}
}

type SetRequest struct {
	Key   string
	Value interface{}
}

type SetResponse struct {
	Key string
}

type SetCommand struct {
	Key   string
	Value interface{}
}
