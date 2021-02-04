package mongoarchivereader

import (
	"fmt"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/options"
)

// tool specific custom options
type CustomOptions struct {
	Out     string `long:"out" value-name:"<directory-path>" short:"o" description:"output directory to place reconstructed archive (default: '<archive>.dump')"`
	Gzip    bool   `long:"gzip" description:"supplied archive is gzipped"`
	Archive string `long:"archive" value-name:"<file-path>" description:"archive to be processed"`
	List    bool   `long:"list" description:"list contents of archive instead of reconstructing it"`
}

func (*CustomOptions) Name() string {
	return "tool specific"
}

type Options struct {
	*options.ToolOptions
	*CustomOptions
}

func ParseOptions(rawArgs []string, versionStr, gitCommit string) (Options, error) {
	var usage = `<options> 

Take an archive from mongodump and list or reconstruct it as a dump of bson and metadata.json files`

	toolOpts := options.New("mongoarchivereader", versionStr, gitCommit, usage, false, options.EnabledOptions{})
	customOpts := &CustomOptions{}
	toolOpts.AddOptions(customOpts)

	args, err := toolOpts.ParseArgs(rawArgs)
	if err != nil {
		return Options{}, fmt.Errorf("error parsing command line options: %v", err)
	}

	log.SetVerbosity(toolOpts.Verbosity)

	if len(args) > 1 {
		return Options{}, fmt.Errorf("too many positional arguments: %v", args)
	}

	// specify default output location
	if customOpts.Out == "" {
		customOpts.Out = customOpts.Archive + ".dump"
		log.Logvf(log.Always, "--out not specified, defaulting to `%s`", customOpts.Out)
	}

	return Options{toolOpts, customOpts}, nil
}
