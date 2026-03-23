package dbrepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSearchPattern(t *testing.T) {
	assert.Equal(t, "%gobo%", BuildSearchPattern("gobo"))
	assert.Equal(t, "", BuildSearchPattern(""))
}

func TestBuildSearchCondition(t *testing.T) {
	t.Run("with_fields", func(t *testing.T) {
		cond := BuildSearchCondition("gobo", "%gobo%", "Name", "Email")
		assert.Contains(t, cond, "Name LIKE @SearchPattern")
		assert.Contains(t, cond, "Email LIKE @SearchPattern")
		assert.Contains(t, cond, " OR ")
	})

	t.Run("empty_search", func(t *testing.T) {
		assert.Equal(t, "1=1", BuildSearchCondition("", "", "Name"))
	})

	t.Run("no_fields", func(t *testing.T) {
		assert.Equal(t, "1=1", BuildSearchCondition("gobo", "%gobo%"))
	})
}

func TestParseSortString(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		fields := ParseSortString("name:asc")
		assert.Len(t, fields, 1)
		assert.Equal(t, "name", fields[0].Column)
		assert.Equal(t, "ASC", fields[0].Direction)
	})

	t.Run("multiple", func(t *testing.T) {
		fields := ParseSortString("name:asc,date:desc")
		assert.Len(t, fields, 2)
		assert.Equal(t, "name", fields[0].Column)
		assert.Equal(t, "ASC", fields[0].Direction)
		assert.Equal(t, "date", fields[1].Column)
		assert.Equal(t, "DESC", fields[1].Direction)
	})

	t.Run("no_direction", func(t *testing.T) {
		fields := ParseSortString("name")
		assert.Len(t, fields, 1)
		assert.Equal(t, "ASC", fields[0].Direction)
	})

	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, ParseSortString(""))
	})
}

func TestBuildOrderByClause(t *testing.T) {
	colMap := map[string]string{
		"name": "Name",
		"date": "CreatedAt",
	}

	t.Run("valid", func(t *testing.T) {
		clause := BuildOrderByClause("name:asc,date:desc", colMap, "ID ASC")
		assert.Equal(t, "ORDER BY Name ASC, CreatedAt DESC", clause)
	})

	t.Run("empty_uses_default", func(t *testing.T) {
		clause := BuildOrderByClause("", colMap, "ID ASC")
		assert.Equal(t, "ORDER BY ID ASC", clause)
	})

	t.Run("unknown_column_uses_default", func(t *testing.T) {
		clause := BuildOrderByClause("unknown:asc", colMap, "ID ASC")
		assert.Equal(t, "ORDER BY ID ASC", clause)
	})

	t.Run("no_default", func(t *testing.T) {
		clause := BuildOrderByClause("", colMap, "")
		assert.Equal(t, "", clause)
	})
}
