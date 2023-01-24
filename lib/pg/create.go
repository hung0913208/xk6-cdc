package pg

import (
	"fmt"
	"strings"
)

func (self *pgImpl) Create(table string, fields []string) error {
	_, err := self.db.Query(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS  %s (%s)",
			table,
			strings.Join(fields, ",")),
	)
	return err
}
