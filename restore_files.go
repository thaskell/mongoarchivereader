package mongoarchivereader

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mongodb/mongo-tools-common/util"
	"github.com/pkg/errors"
)

func (a *Archive) outputPath(dbName, colName string) string {
	// taken from https://github.com/mongodb/mongo-tools/blob/100.2.1/mongodump/prepare.go#L199-L211
	// Encode a new output path for collection names that would result in a file name greater
	// than 255 bytes long. This includes the longest possible file extension: .metadata.json.gz
	// The new format is <truncated-url-encoded-collection-name>%24<collection-name-hash-base64>
	// where %24 represents a $ symbol delimiter (e.g. aVeryVery...VeryLongName%24oPpXMQ...).
	escapedColName := util.EscapeCollectionName(colName)
	if len(escapedColName) > 238 {
		colNameTruncated := escapedColName[:208]
		colNameHashBytes := sha1.Sum([]byte(colName))
		colNameHashBase64 := base64.RawURLEncoding.EncodeToString(colNameHashBytes[:])

		// First 208 bytes of col name + 3 bytes delimiter + 27 bytes base64 hash = 238 bytes max.
		escapedColName = colNameTruncated + "%24" + colNameHashBase64
	}

	return filepath.Join(a.Options.Out, dbName, escapedColName)
}

type restoreFile struct {
	io.WriteCloser
	path string
}

const restoreDirPerm = 0x755

// Create the file to be restored and any directories required that are in it's path
func (rf *restoreFile) Open() error {
	if rf.path == "" {
		return fmt.Errorf("metadata path must not be empty")
	}

	if err := os.MkdirAll(filepath.Dir(rf.path), os.ModeDir|restoreDirPerm); err != nil {
		return errors.Wrapf(err, "failed to create directory for metadata file %q", filepath.Dir(rf.path))
	}

	wc, err := os.Create(rf.path)
	if err != nil {
		return errors.Wrapf(err, "failed to metadata file %q", rf.path)
	}
	rf.WriteCloser = wc

	return nil
}
