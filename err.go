package csvsql

type ErrInvalidQuery struct {
	Message string
}

func (e *ErrInvalidQuery) Error() string {
	return e.Message
}
