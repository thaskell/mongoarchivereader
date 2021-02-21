package mongoarchivereader

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/util"
)

type Archive struct {
	Options *Options
	Reader  *archive.Reader
	Manager *intents.Manager
}

func New(opts *Options) (*Archive, error) {
	a := &Archive{
		Options: opts,
		Manager: intents.NewIntentManager(),
	}

	archiveReader, err := a.Open()
	if err != nil {
		return nil, err
	}

	a.Reader = &archive.Reader{
		In:      archiveReader,
		Prelude: new(archive.Prelude),
	}

	return a, nil
}

// open the archive and handle it being gzipped if necessary
func (a *Archive) Open() (io.ReadCloser, error) {
	rc, err := os.Open(a.Options.Archive)
	if err != nil {
		return nil, err
	}

	if a.Options.Gzip {
		reader, err := gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
		return &util.WrappedReadCloser{ReadCloser: reader, Inner: rc}, nil
	}

	return rc, nil
}
