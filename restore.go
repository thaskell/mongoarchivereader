package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/util"
	"github.com/mongodb/mongo-tools/mongorestore"
)

// Setup the demux and then restore all the intents that have been added to the MongoArchive.Manager.
func (mongoarchive *MongoArchive) ProcessIntents() mongorestore.Result {
	// demux code taken from https://github.com/mongodb/mongo-tools/blob/100.2.1/mongorestore/mongorestore.go#L482-L524
	demuxFinished := make(chan interface{})
	var demuxErr error
	namespaceChan := make(chan string, 1)
	namespaceErrorChan := make(chan error)
	mongoarchive.Archive.Demux.NamespaceChan = namespaceChan
	mongoarchive.Archive.Demux.NamespaceErrorChan = namespaceErrorChan

	go func() {
		demuxErr = mongoarchive.Archive.Demux.Run()
		close(demuxFinished)
	}()
	// consume the new namespace announcement from the demux for all of the special collections
	// that get cached when being read out of the archive.
	// The first regular collection found gets pushed back on to the namespaceChan
	// consume the new namespace announcement from the demux for all of the collections that get cached
	for {
		ns, ok := <-namespaceChan
		// the archive can have only special collections. In that case we keep reading until
		// the namespaces are exhausted, indicated by the namespaceChan being closed.
		if !ok {
			break
		}
		intent := mongoarchive.Manager.IntentForNamespace(ns)
		if intent == nil {
			return mongorestore.Result{Err: fmt.Errorf("no intent for collection in archive: %v", ns)}
		}
		if intent.IsSpecialCollection() {
			log.Logvf(log.DebugLow, "special collection %v found", ns)
			namespaceErrorChan <- nil
		} else {
			// Put the ns back on the announcement chan so that the
			// demultiplexer can start correctly
			log.Logvf(log.DebugLow, "first non special collection %v found."+
				" The demultiplexer will handle it and the remainder", ns)
			namespaceChan <- ns
			break
		}
	}

	mongoarchive.Manager.UsePrioritizer(mongoarchive.Archive.Demux.NewPrioritizer(mongoarchive.Manager))
	result := mongoarchive.RestoreIntents()
	combineResults(&result, mongoarchive.RestoreSpecialIntents())

	<-demuxFinished
	return resultWithError(result, demuxErr)
}

func combineResults(original *mongorestore.Result, new mongorestore.Result) {
	original.Successes += new.Successes
	original.Failures += new.Failures
	original.Err = new.Err
}

func logRestore(result mongorestore.Result, ns string) {
	log.Logvf(log.Always, "finished restoring %v (%v %v, %v %v)",
		ns, result.Successes, util.Pluralize(int(result.Successes), "document", "documents"),
		result.Failures, util.Pluralize(int(result.Failures), "failure", "failures"))
}

func resultWithError(result mongorestore.Result, err error) mongorestore.Result {
	result.Err = err
	return result
}

// restore each of the normal intents stored on MongoArchive.Manager
func (mongoarchive *MongoArchive) RestoreIntents() mongorestore.Result {
	var totalResult mongorestore.Result
	var ioBuf []byte
	for {
		intent := mongoarchive.Manager.Pop()
		if intent == nil {
			break
		}
		if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
			if ioBuf == nil {
				ioBuf = make([]byte, db.MaxBSONSize)
			}
			fileNeedsIOBuffer.TakeIOBuffer(ioBuf)
		}
		result := mongoarchive.RestoreIntent(intent)
		logRestore(result, intent.Namespace())
		combineResults(&totalResult, result)
		mongoarchive.Manager.Finish(intent)
		if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
			fileNeedsIOBuffer.ReleaseIOBuffer()
		}
	}

	return totalResult
}

func (mongoarchive *MongoArchive) RestoreIntent(intent *intents.Intent) mongorestore.Result {
	var result mongorestore.Result
	if intent.MetadataFile != nil {
		err := mongoarchive.restoreMetadata(intent)
		if err != nil {
			return resultWithError(result, err)
		}
		log.Logvf(log.Always, "finished restoring metadata for %s", intent.Namespace())
	}

	if intent.BSONFile != nil {
		return mongoarchive.restoreBSON(intent)
	}

	log.Logvf(log.DebugLow, "intent was neither a metadatafile or bsonfile which is unexpected: %+v", intent)
	return result
}

type specialIntents map[string]*intents.Intent

func (mongoarchive *MongoArchive) getSpecialIntents() specialIntents {
	si := make(specialIntents)
	si["users"] = mongoarchive.Manager.Users()
	si["roles"] = mongoarchive.Manager.Roles()
	si["authversion"] = mongoarchive.Manager.AuthVersion()
	si["oplog"] = mongoarchive.Manager.Oplog()

	return si
}

// MongoArchiver.Manager has special intents stored separately since they are supposed to be restored
// in different ways. Because our output is a file we can process these all the same way
func (mongoarchive *MongoArchive) RestoreSpecialIntents() mongorestore.Result {
	si := mongoarchive.getSpecialIntents()
	var totalResult mongorestore.Result

	var ioBuf []byte
	for name, intent := range si {
		if intent == nil {
			log.Logvf(log.DebugLow, "no intent found for %s, must not have been in the archive, skipping", name)
			continue
		}
		if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
			if ioBuf == nil {
				ioBuf = make([]byte, db.MaxBSONSize)
			}
			fileNeedsIOBuffer.TakeIOBuffer(ioBuf)
		}
		result := mongoarchive.restoreBSON(intent)
		logRestore(result, intent.Namespace())
		combineResults(&totalResult, result)
		if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
			fileNeedsIOBuffer.ReleaseIOBuffer()
		}
	}

	return totalResult
}
