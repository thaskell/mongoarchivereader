package mongoarchivereader

import (
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/pkg/errors"
)

type PreludeFiles []archive.DirLike

// Iterate over the contents of the prelude and return all the files found
func (a *Archive) GetFilesFromPrelude(prelude *archive.PreludeExplorer) (PreludeFiles, error) {
	var files PreludeFiles
	base, err := prelude.ReadDir()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read from %q", prelude.Name())
	}
	for _, baseEntry := range base {
		switch {
		case baseEntry.Name() == "":
			// todo: checking for this case is probably unnecessary now, with the switch
			log.Logvf(log.DebugLow, "entry %q in archive has no name, skipping", baseEntry.Path())

		case baseEntry.IsDir():
			entries, err := baseEntry.ReadDir()
			if err != nil {
				return nil, errors.Wrapf(err, "failed to read %q", prelude.Name())
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					files = append(files, entry)
				}
			}

		case baseEntry.Name() == "oplog.bson":
			// the oplog is special and should be the only non directory file in the base of the archive
			files = append(files, baseEntry)
		}
	}
	return files, nil
}

func (pf *PreludeFiles) PrintFiles() {
	for _, file := range *pf {
		log.Logv(log.Always, file.Path())
	}
}

// create an intent from each file from the archive that is provided
func (a *Archive) CreateIntents(files PreludeFiles) error {
	a.Reader.Demux = archive.CreateDemux(a.Reader.Prelude.NamespaceMetadatas, a.Reader.In)

	for _, file := range files {
		if err := a.createIntent(file); err != nil {
			return err
		}
	}

	return nil
}
