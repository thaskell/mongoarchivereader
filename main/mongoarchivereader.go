package main

import (
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/signals"
	"github.com/mongodb/mongo-tools-common/util"
	"mongoarchivereader"
	"os"
)

func main() {
	opts, err := mongoarchivereader.ParseOptions(os.Args[1:], "", "")
	if err != nil {
		log.Logvf(log.Always, "%v", err)
		log.Logvf(log.Always, util.ShortUsage("mongoarchivereader"))
		os.Exit(util.ExitFailure)
	}

	if opts.PrintHelp(false) {
		return
	}
	if opts.PrintVersion() {
		return
	}

	if opts.Archive == "" {
		log.Logv(log.Always, "archive flag not provided")
		log.Logvf(log.Always, util.ShortUsage("mongoarchivereader"))
		os.Exit(util.ExitFailure)
	}

	// if we are not just listing the contents of the archive we need to make sure
	// the expected output directory doesn't exist or we might overwrite data in it
	if !opts.List {
		_, err := os.Stat(opts.Out)
		if err == nil {
			log.Logvf(log.Always, "--out location of `%s` already exists, not continuing", opts.Out)
			os.Exit(util.ExitFailure)
		}
	}

	signals.Handle()

	mongoarchive, err := mongoarchivereader.New(opts)
	if err != nil {
		log.Logvf(log.Always, "couldn't process archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	err = mongoarchive.Archive.Prelude.Read(mongoarchive.Archive.In)
	if err != nil {
		log.Logvf(log.Always, "couldn't validate archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	prelude, err := mongoarchive.Archive.Prelude.NewPreludeExplorer()
	if err != nil {
		log.Logvf(log.Always, "unable to read archive information: %v", err)
		os.Exit(util.ExitFailure)
	}

	archiveContents, err := mongoarchive.GetFilesFromPrelude(prelude)
	if err != nil {
		log.Logvf(log.Always, "couldn't get files from archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	if mongoarchive.Options.List {
		archiveContents.PrintFiles()
		return
	}

	err = mongoarchive.CreateIntents(archiveContents)
	if err != nil {
		log.Logvf(log.Always, "couldn't process archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	result := mongoarchive.ProcessIntents()
	if result.Err != nil {
		log.Logvf(log.Always, "failed: %v", result.Err)
		os.Exit(util.ExitFailure)
	}
	os.Exit(util.ExitSuccess)
}
