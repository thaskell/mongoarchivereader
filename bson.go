package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools/mongorestore"
	"io"
	"path/filepath"
)

// create intent from BSON in archive and store it in MongoArchive.Manager
func (mongoarchive *MongoArchive) processBSON(sourceNS namespace, file archive.DirLike) {
	intent := &intents.Intent{
		DB:   sourceNS.DB,
		C:    sourceNS.Collection,
		Size: file.Size(),
	}

	intent.Location = filepath.Join(mongoarchive.Options.Out, file.Path())

	if intent.IsSpecialCollection() {
		specialCollectionCache := archive.NewSpecialCollectionCache(intent, mongoarchive.Archive.Demux)
		intent.BSONFile = specialCollectionCache
		mongoarchive.Archive.Demux.Open(intent.Namespace(), specialCollectionCache)
	} else {
		intent.BSONFile = &archive.RegularCollectionReceiver{
			Origin: intent.Namespace(),
			Intent: intent,
			Demux:  mongoarchive.Archive.Demux,
		}
	}

	mongoarchive.Manager.Put(intent)
}

// read the BSON file from the archive and write it to disk
func (mongoarchive *MongoArchive) restoreBSON(intent *intents.Intent) mongorestore.Result {
	var result mongorestore.Result

	err := intent.BSONFile.Open()
	if err != nil {
		return mongorestore.Result{Err: fmt.Errorf("couldn't open bson file `%s`: %v", intent.Namespace(), err)}
	}
	defer func() { _ = intent.BSONFile.Close() }()

	log.Logvf(log.DebugLow, "reading `%s` from archive", intent.Namespace())

	bsonSource := db.NewBSONSource(intent.BSONFile)
	defer func() { _ = bsonSource.Close() }()

	bsonWriter := &restoreFile{path: mongoarchive.outputPath(intent.DB, intent.C) + ".bson"}
	err = bsonWriter.Open()
	if err != nil {
		return mongorestore.Result{Err: fmt.Errorf("couldn't open bson file `%s` to write to: %v", bsonWriter.path, err)}
	}
	defer func() {
		closeErr := bsonWriter.Close()
		if err == nil && closeErr != nil {
			result = resultWithError(result, fmt.Errorf("error writing data for collection `%v` to disk: %v", intent.Namespace(), closeErr))
		}
	}()

	log.Logvf(log.Info, "restoring `%s` to `%s`", intent.Namespace(), bsonWriter.path)
	var f io.Writer
	f = bsonWriter
	for {
		doc := bsonSource.LoadNext()
		if doc == nil {
			break
		}
		_, err := f.Write(doc)
		if err != nil {
			result.Failures++
			result.Err = fmt.Errorf("couldn't write to bson file `%s`: %v", bsonWriter.path, err)
		}
		result.Successes++
	}

	return result
}
