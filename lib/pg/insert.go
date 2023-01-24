package pg

import (
	"fmt"
	"strings"
)

func (self *pgImpl) Insert(table string, fields [][]string) error {
	records := make([]string, len(fields))

	for i := 0; i < len(fields); i++ {
		records[i] = "(" + strings.Join(fields[i], ",") + ")"
	}

	_, err := self.db.Query(
		fmt.Sprintf("INSERT INTO %s VALUES %s",
			table,
			strings.Join(records, ",")),
	)
	return err
}
