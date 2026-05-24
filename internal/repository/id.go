package repository

import "strings"

// normalizeUserIDForDB keeps the domain/API user identifier as string while
// writing/looking up MySQL BIGINT user columns with their numeric value. It is
// intentionally permissive to support the existing in-memory seed format
// (u_1001) and MySQL demo numeric IDs (1/2/3) without a broad domain refactor.
func normalizeUserIDForDB(id string) string {
	id = strings.TrimSpace(id)
	for _, prefix := range []string{"u_", "U_"} {
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix)
		}
	}
	return id
}
