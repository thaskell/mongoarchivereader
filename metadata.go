package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"io"
	"io/ioutil"
	"path/filepath"
)

// create intent from metadata in archive and store it in MongoArchive.Manager
func (mongoarchive *MongoArchive) processMetadata(sourceNS namespace, file archive.DirLike) {
	intent := &intents.Intent{
		DB: sourceNS.DB,
		C:  sourceNS.Collection,
	}
	intent.MetadataFile = &archive.MetadataPreludeFile{
		Origin:  sourceNS.String(),
		Intent:  intent,
		Prelude: mongoarchive.Archive.Prelude,
	}

	intent.MetadataLocation = filepath.Join(mongoarchive.Options.Out, file.Path())
	mongoarchive.Manager.PutWithNamespace(sourceNS.String(), intent)
}

// read the json metadata from the archive and write it to disk
func (mongoarchive *MongoArchive) restoreMetadata(intent *intents.Intent) error {
	err := intent.MetadataFile.Open()
	if err != nil {
		return fmt.Errorf("couldn't open metadata intent `%s`: %v", intent.Namespace(), err)
	}
	defer func() { _ = intent.MetadataFile.Close() }()

	log.Logvf(log.DebugLow, "reading metadata for `%s` from archive", intent.Namespace())

	jsonBytes, err := ioutil.ReadAll(intent.MetadataFile)
	if err != nil {
		return fmt.Errorf("error reading metadata from `%s`: %v", intent.Namespace(), err)
	}

	fileWriter := &restoreFile{path: mongoarchive.outputPath(intent.DB, intent.C) + ".metadata.json"}

	err = fileWriter.Open()
	if err != nil {
		return fmt.Errorf("couldn't open metadata file `%s` for writing: %v", fileWriter.path, err)
	}
	defer func() {
		closeErr := fileWriter.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("error writing metadata for collection `%v` to disk: %v", intent.Namespace(), closeErr)
		}
	}()

	log.Logvf(log.Info, "restoring `%s` to `%s`", intent.Namespace(), fileWriter.path)
	var f io.Writer
	f = fileWriter
	_, err = f.Write(jsonBytes)
	if err != nil {
		return fmt.Errorf("error writing metadata for collection `%s` to disk: %v", intent.Namespace(), err)
	}
	return nil
}
