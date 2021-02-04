package mongoarchivereader

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/mongodb/mongo-tools-common/util"
	"io"
	"os"
	"path/filepath"
)

func (mongoarchive *MongoArchive) outputPath(dbName, colName string) string {
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

	return filepath.Join(mongoarchive.Options.Out, dbName, escapedColName)
}

type restoreFile struct {
	io.WriteCloser
	path string
}

// Create the file to be restored and any directories required that are in it's path
func (restoreFile *restoreFile) Open() error {
	if restoreFile.path == "" {
		return fmt.Errorf("no metadata path supplied")
	}
	err := os.MkdirAll(filepath.Dir(restoreFile.path), os.ModeDir|os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating directory for metadata file `%s`: %v",
			filepath.Dir(restoreFile.path), err)
	}

	restoreFile.WriteCloser, err = os.Create(restoreFile.path)
	if err != nil {
		return fmt.Errorf("error creating metadata file `%s`: %v", restoreFile.path, err)
	}
	return nil
}
