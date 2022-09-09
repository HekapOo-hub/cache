package model

import (
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack"
)

type User struct {
	ID    uuid.UUID `json:"id"`
	Name  string    `json:"name"`
	Age   int       `json:"age"`
	Email string    `json:"email"`
}

func (u *User) MarshalBinary() ([]byte, error) {
	return msgpack.Marshal(u)
}

// UnmarshalBinary unmarshaler for Price
func (u *User) UnmarshalBinary(data []byte) error {
	return msgpack.Unmarshal(data, u)
}
