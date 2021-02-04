package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/archive"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools/mongorestore"
	"path/filepath"
	"strings"
)

type namespace struct {
	DB, Collection string
}

func (ns *namespace) String() string {
	if ns.DB == "" {
		return fmt.Sprintf("%s", ns.Collection)
	}
	return fmt.Sprintf("%s.%s", ns.DB, ns.Collection)
}

func (mongoarchive *MongoArchive) createIntent(file archive.DirLike) error {
	log.Logvf(log.DebugHigh, "processing %s", file.Path())
	collection, fileType, err := mongoarchive.getInfoFromFile(file.Path())
	if err != nil {
		return fmt.Errorf("couldn't get information about %s: %v", file.Path(), err)
	}

	sourceNS := namespace{file.Parent().Name(), collection}
	log.Logvf(log.DebugHigh, "source NS %s", sourceNS)

	switch fileType {
	case mongorestore.BSONFileType:
		mongoarchive.processBSON(sourceNS, file)
	case mongorestore.MetadataFileType:
		mongoarchive.processMetadata(sourceNS, file)
	}
	return nil
}

// Returns the name of the collection and the type of file it is, both parsed from the filename
func (mongoarchive *MongoArchive) getInfoFromFile(filename string) (string, mongorestore.FileType, error) {
	baseFileName := filepath.Base(filename)
	var collName string

	fileType := mongorestore.UnknownFileType

	if strings.HasSuffix(baseFileName, ".metadata.json") {
		collName = strings.TrimSuffix(baseFileName, ".metadata.json")
		fileType = mongorestore.MetadataFileType
	} else if strings.HasSuffix(baseFileName, ".bson") {
		collName = strings.TrimSuffix(baseFileName, ".bson")
		fileType = mongorestore.BSONFileType
	}
	return collName, fileType, nil
}
