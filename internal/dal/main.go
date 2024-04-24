package dal

import (
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InternalId uuid.UUID

func (u *InternalId) UnmarshalJson(b []byte) error {
	if id, err := uuid.Parse(string(b[:])); err != nil {
		return errors.New("Could not parse UUID: " + err.Error())
	} else {
		*u = InternalId(id)
	}
	return nil
}

var DbPool *pgxpool.Pool
