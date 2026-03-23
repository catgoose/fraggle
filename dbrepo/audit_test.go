package dbrepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetCreateTimestamps(t *testing.T) {
	var created, updated time.Time
	SetCreateTimestamps(&created, &updated)
	assert.False(t, created.IsZero())
	assert.False(t, updated.IsZero())
	assert.Equal(t, created, updated)
}

func TestSetUpdateTimestamp(t *testing.T) {
	var updated time.Time
	SetUpdateTimestamp(&updated)
	assert.False(t, updated.IsZero())
}

func TestSetSoftDelete(t *testing.T) {
	var deleted time.Time
	SetSoftDelete(&deleted)
	assert.False(t, deleted.IsZero())
}

func TestInitVersion(t *testing.T) {
	var v int
	InitVersion(&v)
	assert.Equal(t, 1, v)
}

func TestIncrementVersion(t *testing.T) {
	v := 1
	IncrementVersion(&v)
	assert.Equal(t, 2, v)
}

func TestSetStatus(t *testing.T) {
	var s string
	SetStatus(&s, "active")
	assert.Equal(t, "active", s)
}

func TestSetSortOrder(t *testing.T) {
	var o int
	SetSortOrder(&o, 5)
	assert.Equal(t, 5, o)
}

func TestAuditHelpers(t *testing.T) {
	var createdBy, updatedBy string
	SetCreateAudit(&createdBy, &updatedBy, "admin")
	assert.Equal(t, "admin", createdBy)
	assert.Equal(t, "admin", updatedBy)

	SetUpdateAudit(&updatedBy, "user1")
	assert.Equal(t, "user1", updatedBy)
}

func TestDeleteAudit(t *testing.T) {
	var deletedAt time.Time
	var deletedBy string
	SetDeleteAudit(&deletedAt, &deletedBy, "admin")
	assert.False(t, deletedAt.IsZero())
	assert.Equal(t, "admin", deletedBy)
}

func TestArchive(t *testing.T) {
	var archivedAt time.Time
	SetArchive(&archivedAt)
	assert.False(t, archivedAt.IsZero())

	ClearArchive(&archivedAt)
	assert.True(t, archivedAt.IsZero())
}

func TestExpiry(t *testing.T) {
	var expiresAt time.Time
	future := time.Now().Add(24 * time.Hour)
	SetExpiry(&expiresAt, future)
	assert.Equal(t, future, expiresAt)

	ClearExpiry(&expiresAt)
	assert.True(t, expiresAt.IsZero())
}

func TestReplacement(t *testing.T) {
	var replacedBy int64
	SetReplacement(&replacedBy, 42)
	assert.Equal(t, int64(42), replacedBy)

	ClearReplacement(&replacedBy)
	assert.Equal(t, int64(0), replacedBy)
}

func TestNilSafety(t *testing.T) {
	// All helpers should be safe to call with nil pointers
	SetCreateTimestamps(nil, nil)
	SetUpdateTimestamp(nil)
	SetSoftDelete(nil)
	InitVersion(nil)
	IncrementVersion(nil)
	SetSortOrder(nil, 0)
	SetStatus(nil, "")
	SetExpiry(nil, time.Time{})
	SetReplacement(nil, 0)
	ClearReplacement(nil)
	SetArchive(nil)
	ClearArchive(nil)
	ClearExpiry(nil)
	SetCreateAudit(nil, nil, "")
	SetUpdateAudit(nil, "")
	SetDeleteAudit(nil, nil, "")
}
