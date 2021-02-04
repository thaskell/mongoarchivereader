package mongoarchivereader

import (
	"compress/gzip"
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/util"
	"io"
	"os"
)

type MongoArchive struct {
	Archive *archive.Reader
	Options *Options
	Manager *intents.Manager
}

func New(opts Options) (MongoArchive, error) {
	mongoarchive := MongoArchive{Options: &opts}

	mongoarchive.Manager = intents.NewIntentManager()

	archiveReader, err := mongoarchive.GetArchiveReader()
	if err != nil {
		return MongoArchive{}, err
	}

	mongoarchive.Archive = &archive.Reader{
		In:      archiveReader,
		Prelude: &archive.Prelude{},
	}

	return mongoarchive, nil
}

// open the archive and handle it being gzipped if necessary
func (mongoarchive *MongoArchive) GetArchiveReader() (rc io.ReadCloser, err error) {
	rc, err = os.Open(mongoarchive.Options.Archive)
	if err != nil {
		return nil, err
	}

	if mongoarchive.Options.Gzip {
		gzrc, err := gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
		return &util.WrappedReadCloser{ReadCloser: gzrc, Inner: rc}, nil
	}
	return rc, nil
}
