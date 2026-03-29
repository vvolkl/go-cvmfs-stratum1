package catalog

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// TaggedRoot represents a named snapshot (tag) from the CVMFS history
// database and the root catalog hash it points to.
type TaggedRoot struct {
	Name     string // tag name
	Hash     string // hex root catalog hash
	Revision int64  // repository revision at the time of tagging
}

// HistoryConfig holds paths needed to open and decompress the history DB.
type HistoryConfig struct {
	// DataDir is the repository data directory (contains 00..ff subdirs).
	DataDir string
	// TempDir is where temporary decompressed files are placed.
	TempDir string
}

// ReadTaggedRoots opens the CVMFS history database identified by
// historyHash (with suffix 'H'), decompresses it if needed, and returns
// every distinct tagged root catalog hash.
//
// The history database is a SQLite file with a `tags` table:
//
//	CREATE TABLE tags (
//	    name TEXT PRIMARY KEY,
//	    hash TEXT,
//	    revision INTEGER,
//	    timestamp INTEGER,
//	    channel INTEGER,
//	    description TEXT,
//	    size INTEGER,
//	    branch TEXT
//	);
//
// Each row's `hash` column is the hex content hash of the root catalog
// for that named snapshot.
func ReadTaggedRoots(cfg HistoryConfig, historyHash string) ([]TaggedRoot, error) {
	if len(historyHash) < 3 {
		return nil, fmt.Errorf("history hash too short: %s", historyHash)
	}

	// The history object is stored with suffix 'H'.
	objPath := filepath.Join(cfg.DataDir, historyHash[:2], historyHash[2:]+string(SuffixHistory))

	// Prepare the DB (decompress if needed) using the same logic as
	// prepareCatalogDB.
	dbPath, cleanup, err := prepareDB(cfg.TempDir, objPath)
	if err != nil {
		return nil, fmt.Errorf("preparing history db %s: %w", historyHash, err)
	}
	defer cleanup()

	db, err := sql.Open("sqlite", dbPath+"?mode=ro&_journal_mode=OFF&_synchronous=OFF")
	if err != nil {
		return nil, fmt.Errorf("opening history db: %w", err)
	}
	defer db.Close()

	// Check that the tags table exists (older repos may not have one).
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='tags'").Scan(&tableName)
	if err != nil {
		log.Printf("History DB %s has no tags table — skipping", historyHash[:16])
		return nil, nil
	}

	rows, err := db.Query("SELECT DISTINCT name, hash, revision FROM tags WHERE hash IS NOT NULL AND hash != '' ORDER BY revision ASC")
	if err != nil {
		return nil, fmt.Errorf("querying tags: %w", err)
	}
	defer rows.Close()

	var roots []TaggedRoot
	seen := make(map[string]bool)
	for rows.Next() {
		var t TaggedRoot
		if err := rows.Scan(&t.Name, &t.Hash, &t.Revision); err != nil {
			return nil, fmt.Errorf("scanning tag row: %w", err)
		}
		if t.Hash == "" {
			continue
		}
		// Deduplicate by hash — multiple tags can point to the same
		// root catalog.  We keep the first (lowest revision) name.
		if !seen[t.Hash] {
			seen[t.Hash] = true
			roots = append(roots, t)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating tags: %w", err)
	}

	return roots, nil
}

// prepareDB makes a (potentially compressed) SQLite file available for
// reading.  This is a generalised version of prepareCatalogDB that works
// for any CVMFS SQLite object (catalogs, history DBs, etc.).
func prepareDB(tempDir, objPath string) (string, func(), error) {
	noop := func() {}

	if _, err := os.Stat(objPath); os.IsNotExist(err) {
		return "", noop, fmt.Errorf("file not found: %s", objPath)
	}

	f, err := os.Open(objPath)
	if err != nil {
		return "", noop, err
	}

	magic := make([]byte, 16)
	n, err := f.Read(magic)
	f.Close()
	if err != nil {
		return "", noop, err
	}

	if n >= 16 && string(magic[:16]) == "SQLite format 3\000" {
		return objPath, noop, nil
	}

	// Compressed — decompress to a temp file.
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	tmpFile, err := os.CreateTemp(tempDir, "cvmfs-historydb-*.sqlite")
	if err != nil {
		return "", noop, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	cleanup := func() { os.Remove(tmpPath) }

	if err := decompressZlib(objPath, tmpFile); err != nil {
		tmpFile.Close()
		cleanup()
		return "", noop, fmt.Errorf("decompressing: %w", err)
	}
	tmpFile.Close()

	return tmpPath, cleanup, nil
}
