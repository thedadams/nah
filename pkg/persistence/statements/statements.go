package statements

import (
	"bytes"
	"embed"
)

//go:embed *.sql
var fs embed.FS

type Statements struct {
	statements map[string]string
}

func New() *Statements {
	s := &Statements{
		statements: map[string]string{},
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		panic("failed to read sql files: " + err.Error())
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sql, err := fs.ReadFile(entry.Name())
		if err != nil {
			panic("failed to read sql file: " + err.Error())
		}
		s.statements[entry.Name()] = string(bytes.TrimSpace(sql))
	}

	return s
}

func (s *Statements) CreateTable() string {
	return s.statements["create_revisions_table.sql"]
}

func (s *Statements) UpdateLatestRevision() string {
	return s.statements["update_latest_revision.sql"]
}

func (s *Statements) GetLatestRevision() string {
	return s.statements["get_latest_revision.sql"]
}
