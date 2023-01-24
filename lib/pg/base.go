package pg

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type Pg interface {
	Open(connectionString string) error
	Close() error
	Create(table string, fields []string) error
	Insert(table string, fields [][]string) error
}

type pgImpl struct {
	db *sql.DB
}

func NewPg() Pg {
	return &pgImpl{}
}

func (self *pgImpl) Open(connectionString string) error {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	self.db = db
	return nil
}

func (self *pgImpl) Close() error {
	return self.db.Close()
}
