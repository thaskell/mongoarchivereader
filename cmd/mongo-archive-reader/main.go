// Command mongoarchivereader does $stuff.
package main

import (
	"os"

	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/signals"
	"github.com/mongodb/mongo-tools-common/util"
	mar "github.com/thaskell/mongo-archive-reader"
)

const (
	// todo: go tools usually use dashes, e.g. mongo-archive-reader
	toolName = "mongoarchivereader"
)

func main() {
	opts, err := mar.ParseOptions(os.Args[1:], "", "")
	if err != nil {
		log.Logvf(log.Always, "%v", err)
		log.Logvf(log.Always, util.ShortUsage(toolName))
		os.Exit(util.ExitFailure)
	}

	if opts.PrintHelp(false) {
		return
	}

	if opts.PrintVersion() {
		return
	}

	if opts.Archive == "" {
		log.Logv(log.Always, "archive flag required")
		log.Logvf(log.Always, util.ShortUsage(toolName))
		os.Exit(util.ExitFailure)
	}

	// if we are not just listing the contents of the archive we need to make sure
	// the expected output directory doesn't exist or we might overwrite data in it
	if !opts.List {
		if _, err := os.Stat(opts.Out); err != nil {
			log.Logvf(log.Always, "--out location of `%s` already exists, refusing to overwrite", opts.Out)
			os.Exit(util.ExitFailure)
		}
	}

	signals.Handle()

	archive, err := mar.New(opts)
	if err != nil {
		log.Logvf(log.Always, "failed to process archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	if err = archive.Reader.Prelude.Read(archive.Reader.In); err != nil {
		log.Logvf(log.Always, "failed to validate archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	prelude, err := archive.Reader.Prelude.NewPreludeExplorer()
	if err != nil {
		log.Logvf(log.Always, "failed to read archive information: %v", err)
		os.Exit(util.ExitFailure)
	}

	archiveContents, err := archive.GetFilesFromPrelude(prelude)
	if err != nil {
		log.Logvf(log.Always, "failed to get files from archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	if archive.Options.List {
		archiveContents.PrintFiles()
		return
	}

	if err = archive.CreateIntents(archiveContents); err != nil {
		log.Logvf(log.Always, "failed to process archive: %v", err)
		os.Exit(util.ExitFailure)
	}

	if result := archive.ProcessIntents(); result.Err != nil {
		log.Logvf(log.Always, "failed to process intents: %v", result.Err)
		os.Exit(util.ExitFailure)
	}
}
