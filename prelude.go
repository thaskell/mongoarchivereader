package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/log"
)

type PreludeFiles []archive.DirLike

// Iterate over the contents of the prelude and return all the files found
func (mongoarchive *MongoArchive) GetFilesFromPrelude(prelude *archive.PreludeExplorer) (PreludeFiles, error) {
	var files PreludeFiles
	base, err := prelude.ReadDir()
	if err != nil {
		return PreludeFiles{}, fmt.Errorf("couldn't read from %s: %v", prelude.Name(), err)
	}
	for _, baseEntry := range base {
		if baseEntry.Name() == "" {
			log.Logv(log.DebugLow, "entry in archive has no name, skipping")
			continue
		}
		if baseEntry.IsDir() {
			baseEntryContent, err := baseEntry.ReadDir()
			if err != nil {
				return PreludeFiles{}, fmt.Errorf("couldn't read from %s: %v", prelude.Name(), err)
			}
			for _, entry := range baseEntryContent {
				if entry.IsDir() {
					log.Logvf(log.DebugLow, "%s shouldn't contain a directory but %s was found, skipping", baseEntry.Name(), entry.Name())
					continue
				}
				files = append(files, entry)
			}
		}
		// the oplog is special and should be the only non directory file in the base of the archive
		if baseEntry.Name() == "oplog.bson" {
			files = append(files, baseEntry)
		}
	}
	return files, nil
}

func (files *PreludeFiles) PrintFiles() {
	for _, file := range *files {
		log.Logv(log.Always, file.Path())
	}
}

// create an intent from each file from the archive that is provided
func (mongoarchive *MongoArchive) CreateIntents(files PreludeFiles) error {
	mongoarchive.Archive.Demux = archive.CreateDemux(mongoarchive.Archive.Prelude.NamespaceMetadatas, mongoarchive.Archive.In)

	for _, file := range files {
		err := mongoarchive.createIntent(file)
		if err != nil {
			return err
		}
	}
	return nil
}
