package mongoarchivereader

import (
	"path/filepath"

	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
	"gophers.dev/pkgs/ignore"
)

// create intent from BSON in archive and store it in Archive.Manager
func (a *Archive) processBSON(sourceNS namespace, file archive.DirLike) {
	intent := &intents.Intent{
		DB:       sourceNS.DB,
		C:        sourceNS.Collection,
		Size:     file.Size(),
		Location: filepath.Join(a.Options.Out, file.Path()),
	}

	if intent.IsSpecialCollection() {
		specialCollectionCache := archive.NewSpecialCollectionCache(intent, a.Reader.Demux)
		intent.BSONFile = specialCollectionCache
		a.Reader.Demux.Open(intent.Namespace(), specialCollectionCache)
	} else {
		intent.BSONFile = &archive.RegularCollectionReceiver{
			Origin: intent.Namespace(),
			Intent: intent,
			Demux:  a.Reader.Demux,
		}
	}

	a.Manager.Put(intent)
}

// read the BSON file from the archive and write it to disk
func (a *Archive) restoreBSON(intent *intents.Intent) mongorestore.Result {
	var result mongorestore.Result

	if err := intent.BSONFile.Open(); err != nil {
		return mongorestore.Result{Err: errors.Wrapf(err, "failed to open %q", intent.Namespace())}
	}
	defer ignore.Close(intent.BSONFile)

	log.Logvf(log.DebugLow, "reading %q from archive", intent.Namespace())

	bsonSource := db.NewBSONSource(intent.BSONFile)
	defer ignore.Close(bsonSource)

	bsonWriter := &restoreFile{path: a.outputPath(intent.DB, intent.C) + ".bson"}
	if err := bsonWriter.Open(); err != nil {
		return mongorestore.Result{Err: errors.Wrapf(err, "failed to open %q", bsonWriter.path)}
	}
	defer func() {
		if err := bsonWriter.Close(); err != nil {
			result = resultWithError(result, errors.Wrapf(err, "failed to write data for collection %q to disk", intent.Namespace()))
		}
	}()

	log.Logvf(log.Info, "restoring %q to %q", intent.Namespace(), bsonWriter.path)

	for {
		doc := bsonSource.LoadNext()
		if doc == nil {
			break
		}
		if _, err := bsonWriter.Write(doc); err != nil {
			result.Failures++
			result.Err = errors.Wrapf(err, "failed to write to bson file %q", bsonWriter.path)
		}
		result.Successes++
	}

	return result
}
