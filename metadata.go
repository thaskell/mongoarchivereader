package mongoarchivereader

import (
	"io/ioutil"
	"path/filepath"

	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/intents"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/pkg/errors"
	"gophers.dev/pkgs/ignore"
)

// create intent from metadata in archive and store it in Archive.Manager
func (a *Archive) processMetadata(sourceNS namespace, file archive.DirLike) {
	intent := &intents.Intent{
		DB: sourceNS.DB,
		C:  sourceNS.Collection,
	}
	intent.MetadataFile = &archive.MetadataPreludeFile{
		Origin:  sourceNS.String(),
		Intent:  intent,
		Prelude: a.Reader.Prelude,
	}

	intent.MetadataLocation = filepath.Join(a.Options.Out, file.Path())
	a.Manager.PutWithNamespace(sourceNS.String(), intent)
}

// read the json metadata from the archive and write it to disk
func (a *Archive) restoreMetadata(intent *intents.Intent) error {
	if err := intent.MetadataFile.Open(); err != nil {
		return errors.Wrapf(err, "failed to open metadata intent %q", intent.Namespace())
	}
	defer ignore.Close(intent.MetadataFile)

	log.Logvf(log.DebugLow, "reading metadata for %q from archive", intent.Namespace())

	// TODO: lets see if we can use io.Copy to avoid reading everything into memory

	jsonBytes, err := ioutil.ReadAll(intent.MetadataFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read metadata from %q", intent.Namespace())
	}

	fileWriter := &restoreFile{path: a.outputPath(intent.DB, intent.C) + ".metadata.json"}

	if err := fileWriter.Open(); err != nil {
		return errors.Wrapf(err, "failed to open metadata file %q", fileWriter.path)
	}
	defer ignore.Close(fileWriter)

	log.Logvf(log.Info, "restoring %q to %q", intent.Namespace(), fileWriter.path)

	if _, err = fileWriter.Write(jsonBytes); err != nil {
		return errors.Wrapf(err, "failed to write metadata for collection %q", intent.Namespace())
	}

	return nil
}
